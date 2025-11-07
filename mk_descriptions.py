#! /usr/local/bin/python3
"""Create and populate the rule_descriptions table."""

import psycopg
import mk_requirement_dicts
from bisect import bisect
from collections import defaultdict, namedtuple
from psycopg.rows import namedtuple_row

SC = namedtuple('SC', 'course_id offer_nbr min_gpa req_info')
DC = namedtuple('DC', 'course_id offer_nbr is_pseudo req_info')


# min_grade()
# -------------------------------------------------------------------------------------------------
def min_grade(min_gpa) -> str:
  """Convert min_gpa to a letter-grade string.

  If gpa is lt 0.7, assume “any passing grade” (P)
  """
  breakpoints = [0.7, 1.0, 1.3, 1.7, 2.0, 2.3, 2.7, 3.0, 3.3, 3.7, 4.0, 4.3]
  letters = ['P', 'D-', 'D', 'D+', 'C-', 'C', 'C+', 'B-', 'B', 'B+', 'A-', 'A', 'A+']
  return letters[bisect(breakpoints, float(min_gpa))]


# format_requirements()
# -------------------------------------------------------------------------------------------------
def format_requirements(requirements: dict) -> str:
  """Generate a string description from a requirements dict."""
  if not requirements:
    return '--:--:--:000'
  return (f'{requirements['pways'] if requirements['pways'] else '--'}'
          f':{'CO' if requirements['copt'] else '--'}'
          f':{'ME' if requirements['equiv'] else '--'}'
          f':{len(requirements['plans']):03}')


# oxfordize()
# -------------------------------------------------------------------------------------------------
def oxfordize(things: list, conjunction: str = 'and') -> str:
  """Create an string from the list, with an oxford comma if appropriate.
  """
  match len(things):
    case 0:
      return ''
    case 1:
      return things[0]
    case 2:
      return f'{things[0]} {conjunction} {things[1]}'

  return ', '.join(things[0:-1]) + f', {conjunction} {things[-1]}'


# __main__():
# -------------------------------------------------------------------------------------------------
if __name__ == "__main__":
  mk_requirement_dicts.mk_dicts()

  with psycopg.connect("dbname=cuny_curriculum") as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:

      # Cache course info dicts
      """ Info needed to display all three levels of detail, plus status and career for
        anomaly checking (they should 'A' and 'UGRD').
      """
      cursor.execute("""
      select institution, course_id, offer_nbr,
           discipline||' '||catalog_number as course,
           min_credits, max_credits,
           designation in ('MLA', 'MNL') as is_mesg, course_status, career,
           attributes, requirements
        from cuny_courses
      """)
      course_info = defaultdict(dict)
      for row in cursor.fetchall():
        is_pseudo = row.is_mesg or 'BKCR' in row.attributes
        credits = row.max_credits if row.min_credits == row.max_credits else 'varies'
        requirements = row.requirements
        course_info[row.course_id][row.offer_nbr] = {'institution': row.institution,
                                                     'course': row.course,
                                                     'credits': credits,
                                                     'is_pseudo': is_pseudo,
                                                     'status': row.course_status,
                                                     'career': row.career,
                                                     'requirements': requirements}

      # Generate all descriptions
      all_descriptions = []
      cursor.execute("""
      SELECT
        rule_key,
        COALESCE(s.source_courses, '[]'::jsonb)    AS source_courses,
        COALESCE(d.destination_courses, '[]'::jsonb) AS destination_courses
      FROM transfer_rules AS r

      LEFT JOIN LATERAL (
        SELECT jsonb_agg(sc_obj ORDER BY sc_obj->>'course_id', sc_obj->>'offer_nbr')
           AS source_courses
        FROM (
        SELECT DISTINCT jsonb_build_object(
          'course_id',   sc.course_id,
          'offer_nbr',   sc.offer_nbr,
          'min_gpa',   sc.min_gpa
        ) AS sc_obj
        FROM source_courses AS sc
        WHERE sc.rule_id = r.id
        ) AS x
      ) AS s ON TRUE

      LEFT JOIN LATERAL (
        SELECT jsonb_agg(dc_obj ORDER BY dc_obj->>'course_id', dc_obj->>'offer_nbr')
           AS destination_courses
        FROM (
        SELECT DISTINCT jsonb_build_object(
          'course_id', dc.course_id,
          'offer_nbr', dc.offer_nbr,
          'is_mesg',   dc.is_mesg,
          'is_bkcr',   dc.is_bkcr
        ) AS dc_obj
        FROM destination_courses AS dc
        WHERE dc.rule_id = r.id
        ) AS y
      ) AS d ON TRUE

      ORDER BY r.rule_key
      -- LIMIT 10 -- for testing
      """)
      for row in cursor.fetchall():
        if 0 == (cursor.rownumber % 100000):
          print(f'{cursor.rownumber:,}/{cursor.rowcount:,}')
        rule_key = row.rule_key
        src_list = []
        dst_list = []
        # Gather information for all source courses
        source_courses = [(source_course['course_id'], source_course['offer_nbr'],
                          source_course['min_gpa'])
                          for source_course in row.source_courses]
        try:
          for course_id, offer_nbr, min_gpa in source_courses:
            this_course = {'course_id': None,
                           'offer_nbr': None,
                           'course': '',
                           'min_grade': 'P',
                           'aliases': [],
                           'requirements': dict
                           }
            src_infos = course_info[course_id]
            for this_offer_nbr, src_info in src_infos.items():
              if this_offer_nbr == offer_nbr:
                # This _is_ this course: fill in the dict
                this_course['course_id'] = course_id
                this_course['offer_nbr'] = offer_nbr
                this_course['course'] = src_info['course']
                this_course['min_grade'] = min_grade(min_gpa)
                this_course['requirements'] = format_requirements(src_info['requirements'])
              else:
                this_course['aliases'].append(src_info['course'])

            if this_course['offer_nbr'] is None:
              raise KeyError('offer_nbr not in src_infos')

            aliases = (f' (={','.join(this_course['aliases'])})' if this_course['aliases']
                       else '')
            src_list.append(f'{this_course['course']}{aliases} '
                            f'{this_course['min_grade']} ' f'[{this_course['requirements']}]')
        except KeyError as err:
          print(f'src: ‘{err}’ {row.rule_key:20} {course_id:06}:{offer_nbr} {min_gpa:6} '
                f'{src_infos}')

        # Gather the information for all destination courses
        destination_courses = [(destination_course['course_id'],
                                destination_course['offer_nbr'],
                                destination_course['is_mesg'],
                                destination_course['is_bkcr'])
                               for destination_course in row.destination_courses]
        try:
          for course_id, offer_nbr, is_mesg, is_bkcr in destination_courses:
            this_course = {'course_id': None,
                           'offer_nbr': None,
                           'course': '',
                           'mesg': False,
                           'bkcr': False,
                           'aliases': [],
                           'requirements': dict
                           }
            dst_infos = course_info[course_id]
            for this_offer_nbr, dst_info in dst_infos.items():
              if this_offer_nbr == offer_nbr:
                # This _is_ this course: fill in the dict
                this_course['course_id'] = course_id
                this_course['offer_nbr'] = offer_nbr
                this_course['course'] = dst_info['course']
                this_course['mesg'] = 'M' if is_mesg else '-'
                this_course['bkcr'] = 'B' if is_bkcr else '-'
                this_course['requirements'] = format_requirements(dst_info['requirements'])
              else:
                this_course['aliases'].append(dst_info['course'])

            if this_course['offer_nbr'] is None:
              raise KeyError('offer_nbr not in dst_infos')
            aliases = (f' (={','.join(this_course['aliases'])})' if this_course['aliases']
                       else '')
            dst_list.append(f'{this_course['course']}{aliases} '
                            f'{this_course['mesg']}{this_course['bkcr']} '
                            f'[{this_course['requirements']}]')
        except KeyError as err:
          print(f'dst: ‘{err}’ {row.rule_key:20} {course_id:06}:{offer_nbr} {dst_infos}')

        all_descriptions.append((rule_key, f'{oxfordize(src_list)} => {oxfordize(dst_list)}'))

      # Replace all rows in the rule_descriptions table
      cursor.execute('truncate rule_descriptions')
      with cursor.copy('copy rule_descriptions (rule_key, description) from stdin') as cpy:
        for row in all_descriptions:
          cpy.write_row(row)
      print(f'Generted {len(all_descriptions):,} rule_descriptions')
