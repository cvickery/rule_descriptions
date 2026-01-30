#! /usr/local/bin/python3
"""Create and populate the rule_descriptions table for the public schema or an archived set of
   transfer rules.

  Descriptions are always based on the current cuny_courses table, so there will be inaccuracies
  for archived versions of the transfer rules.
"""

import psycopg
import mk_requirement_dicts
import sys

from bisect import bisect
from collections import defaultdict, namedtuple
from datetime import date
from psycopg.rows import namedtuple_row

SC = namedtuple('SC', 'course_id offer_nbr min_gpa req_info')
DC = namedtuple('DC', 'course_id offer_nbr is_pseudo req_info')
error_log = None

mk_requirement_dicts.mk_dicts()
course_info = defaultdict(dict)
with psycopg.connect("dbname=cuny_curriculum") as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    # Cache course info dicts
    cursor.execute("""
    select institution, course_id, offer_nbr,
        discipline||' '||catalog_number as course,
        min_credits, max_credits,
        designation in ('MLA', 'MNL') as is_mesg,
        attributes ~* 'bkcr' as is_bkcr,
        course_status, career,
        requirements
      from cuny_courses
    """)
    for row in cursor.fetchall():
      credits = row.max_credits if row.min_credits == row.max_credits else 'varies'
      requirements = row.requirements
      course_info[row.course_id][row.offer_nbr] = {'institution': row.institution,
                                                   'course': row.course,
                                                   'credits': credits,
                                                   'is_mesg': row.is_mesg,
                                                   'is_bkcr': row.is_bkcr,
                                                   'status': row.course_status,
                                                   'career': row.career,
                                                   'requirements': requirements}


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


# get_rule_info()
# -------------------------------------------------------------------------------------------------
def get_rule_info(rule_key: str) -> namedtuple:
  """Query to get course info for a single rule.

      Uses current information about courses, which might be inappropriate.
  """
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      cursor.execute("""
      SELECT
        r.rule_key,
        COALESCE(s.source_courses, '[]'::jsonb)      AS source_courses,
        COALESCE(d.destination_courses, '[]'::jsonb) AS destination_courses
      FROM transfer_rules AS r
      LEFT JOIN LATERAL (
        SELECT jsonb_agg(sc_obj ORDER BY sc_obj->>'course_id',
                                        sc_obj->>'offer_nbr') AS source_courses
        FROM (
          SELECT DISTINCT jsonb_build_object(
            'course_id', sc.course_id,
            'offer_nbr', sc.offer_nbr,
            'min_gpa',   sc.min_gpa
          ) AS sc_obj
          FROM source_courses AS sc
          WHERE sc.rule_id = r.id
        ) AS x
      ) AS s ON TRUE
      LEFT JOIN LATERAL (
        SELECT jsonb_agg(dc_obj ORDER BY dc_obj->>'course_id',
                                        dc_obj->>'offer_nbr') AS destination_courses
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
      WHERE r.rule_key = %s     -- or = $1
      """, (rule_key,))
      match cursor.rowcount:
        case 0:
          return None
        case 1:
          return cursor.fetchone()
        case _:
          raise ValueError(f'Multiple instances of {rule_key}')


# describe_rule()
# -------------------------------------------------------------------------------------------------
def describe_rule(row: namedtuple) -> tuple:
  if not row:
    return 'No Rule Information'
  src_list = []
  dst_list = []
  # Gather information for all source courses
  source_courses = [(source_course['course_id'], source_course['offer_nbr'],
                    source_course['min_gpa'])
                    for source_course in row.source_courses]

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
      # No matching offer_nbr in src_infos → bogus rule
      this_course['course'] = 'No course'
      this_course['requirements'] = '[--:--:--:---]'
      print(f'src: offer_nbr not in src_infos '
            f'{row.rule_key:20} {course_id:06}:{offer_nbr} {min_gpa:6} {src_infos}',
            file=error_log)

    aliases = (f' (={','.join(this_course['aliases'])})'
               if this_course['aliases'] else '')
    src_list.append(f'{this_course['course']}{aliases} '
                    f'{this_course['min_grade']} '
                    f'[{this_course['requirements']}]'
                    )
  # Gather the information for all destination courses
  destination_courses = [(destination_course['course_id'],
                          destination_course['offer_nbr'])
                         for destination_course in row.destination_courses]
  for course_id, offer_nbr in destination_courses:
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
        this_course['mesg'] = 'M' if dst_info['is_mesg'] else '-'
        this_course['bkcr'] = 'B' if dst_info['is_bkcr'] else '-'
        this_course['requirements'] = format_requirements(dst_info['requirements'])
      else:
        this_course['aliases'].append(dst_info['course'])

    if this_course['offer_nbr'] is None:
      # No matching offer_nbr in dst_infos → bogus rule
      this_course['course'] = 'No course'
      this_course['requirements'] = ''
      print(f'dst: offer_nbr not in src_infos '
            f'{row.rule_key:20} {course_id:06}:{offer_nbr} {min_gpa:6} {src_infos}',
            file=error_log)

    aliases = (f' (={','.join(this_course['aliases'])})' if this_course['aliases']
               else '')
    dst_list.append(f'{this_course['course']}{aliases} '
                    f'{this_course['mesg']}{this_course['bkcr']} '
                    f'[{this_course['requirements']}]')

  return (row.rule_key, row.effective_date, f'{oxfordize(src_list)} => {oxfordize(dst_list)}')


# describe_rules()
# -------------------------------------------------------------------------------------------------
def describe_rules(schema_name: str) -> list:
  """Describe all the rules in a schema."""
  global error_log
  error_log = open(f'./description_errors.{schema_name}.log', 'w')

  all_descriptions = []
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      # Make sure the schema exists
      cursor.execute("""
      select schema_name
        from information_schema.schemata
      where schema_name = %s""", (schema_name, ))
      if cursor.rowcount == 0:
        raise ValueError(f'schema {schema_name} does not exist')

      # Create views to handle difference between public and temp schemata
      if schema_name == 'public':
        cursor.execute("""
        create or replace view public.source_courses_u as
          select tr.rule_key,
                sc.course_id, sc.offer_nbr, sc.max_credits, sc.min_gpa
          from   public.source_courses sc
          join   public.transfer_rules tr on tr.id = sc.rule_id;

        create or replace view public.destination_courses_u as
          select tr.rule_key,
                dc.course_id, dc.offer_nbr
          from   public.destination_courses dc
          join   public.transfer_rules tr on tr.id = dc.rule_id;
        """)
      else:
        cursor.execute(f"""
        create or replace view {schema_name}.source_courses_u as
          select rule_key, course_id, offer_nbr, max_credits, min_gpa
          from   {schema_name}.source_courses;

        create or replace view {schema_name}.destination_courses_u as
          select rule_key, course_id, offer_nbr
          from   {schema_name}.destination_courses;
        """)

      # Gather the information needed to describe the rules
      cursor.execute(f"""
      WITH
      sc AS (
        SELECT rule_key,
              jsonb_agg(
                jsonb_build_object(
                  'course_id', course_id,
                  'offer_nbr', offer_nbr,
                  'min_gpa',   min_gpa,
                  'max_credits', max_credits
                )
                ORDER BY course_id, offer_nbr, coalesce(min_gpa, 0.0), coalesce(max_credits, 99.0)
              ) AS source_courses
        FROM {schema_name}.source_courses_u
        GROUP BY rule_key
      ),
      dc AS (
        SELECT rule_key,
              jsonb_agg(
                jsonb_build_object(
                  'course_id', course_id,
                  'offer_nbr', offer_nbr
                )
                ORDER BY course_id, offer_nbr
              ) AS destination_courses
        FROM {schema_name}.destination_courses_u
        GROUP BY rule_key
      )
      SELECT
        r.rule_key,
        r.effective_date,
        coalesce(sc.source_courses, '[]'::jsonb) AS source_courses,
        coalesce(dc.destination_courses, '[]'::jsonb) AS destination_courses
      FROM {schema_name}.transfer_rules r
      LEFT JOIN sc USING (rule_key)
      LEFT JOIN dc USING (rule_key)
      ORDER BY r.rule_key;

      """)
      for row in cursor.fetchall():
        all_descriptions.append(describe_rule(row))

  return all_descriptions


# __main__():
# -------------------------------------------------------------------------------------------------
if __name__ == "__main__":

  schema_name = sys.argv[1] if len(sys.argv) > 1 else 'public'

  all_descriptions = describe_rules(schema_name)
  with conn.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor() as cursor:
      # (Re)create the rule_descriptions table
      cursor.execute(f"""
        drop table if exists {schema_name}.rule_descriptions;
        create table {schema_name}.rule_descriptions (
          rule_key       text primary key,
          effective_date text,
          description    text
        )
        """)
      with cursor.copy(f'copy {schema_name}.rule_descriptions '
                       f'(rule_key, effective_date, description) from stdin') as cpy:
        for row in all_descriptions:
          cpy.write_row(row)

      if schema_name == 'public':
        cursor.execute("""
        update updates set update_date = %s where table_name = 'rule_descriptions'
        """, (date.today(),))

  print(f'Generated {len(all_descriptions):,} rule_descriptions in schema {schema_name}')
