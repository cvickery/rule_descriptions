#! /usr/local/bin/python3
"""Add and populate requirements column in the cuny_courses table.

   Requirements is a dict of requirement types a course can satisfy, if any. Three keys:
      core: Common Core area abbreviation (String)
      copt: Boolean
      equiv: Major Equivalency names (List of strings)
      plans: Academic plans having at least one requirement the course satisfies (List of strings)
"""

import psycopg
import re
from psycopg.rows import namedtuple_row
from psycopg.types.json import Json


# mk_dicts()
# -------------------------------------------------------------------------------------------------
def mk_dicts():
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      cursor.execute("""
      alter table cuny_courses
      add column if not exists requirements json
      """)

      # Get cuny_course designation (for Pathways) and attributes (for COPT and Major Equivalencies)
      # Get list of programs from dgw.courses
      cursor.execute("""
      SELECT
        c.institution,
        c.course_id,
        c.offer_nbr,
        c.discipline,
        c.catalog_number,
        c.designation,
        c.attributes,
        COALESCE(
          array_agg(DISTINCT dc.plan ORDER BY dc.plan)
            FILTER (WHERE dc.plan IS NOT NULL),
          '{}'
        ) AS plans
      FROM cuny_courses AS c
      LEFT JOIN dgw.courses AS dc
        ON c.course_id = split_part(dc.course_id, ':', 1)::int
      AND c.offer_nbr = split_part(dc.course_id, ':', 2)::int
      WHERE c.career = 'UGRD'
        AND c.course_status = 'A'
      GROUP BY
        c.institution, c.course_id, c.offer_nbr, c.discipline,
        c.catalog_number, c.designation, c.attributes;
      """)
      rows = cursor.fetchall()
      for row in rows:
        requirements = dict()
        attr_str = row.attributes if row.attributes else ''

        # Pathways
        if match := re.match(r'^[RF](..)[CDR]$', row.designation):
          requirements['pways'] = match[1]
        else:
          requirements['pways'] = None

        requirements['copt'] = row.designation.startswith('CO') or 'COPT' in attr_str

        # Major Equivalents
        try:
          attr_dict = dict(part.strip().split(":", 1) for part in attr_str.split(";")
                           if part.strip())
          requirements['equiv'] = [attr_value for attr_key, attr_value in attr_dict.items()
                                   if attr_key.startswith('ME')]
        except ValueError:
          requirements['equiv'] = None

        # Program Requirements
        # Note to self: all rows in dgw.courses have non-empty plan fields
        requirements['plans'] = row.plans

        # Update the cuny_courses.requirements column for this course
        cursor.execute("""
        update cuny_courses set requirements = %s
        where course_id = %s
          and offer_nbr = %s
        """, (Json(requirements), row.course_id, row.offer_nbr))


if __name__ == '__main__':
  mk_dicts()
