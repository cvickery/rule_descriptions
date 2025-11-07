# “Canonical” Transfer Rule Descriptions

## October 29, 2025
I keep generating rule description strings in various projects, in varying formats. This project creates a table of “canonical” descriptions.

  **&lt;source\_courses> => &lt;destination\_courses>**

  - Both &lt;source\_courses> and &lt;destination\_courses> are comma-separated *and* lists of _course\_items_
  - Each _course\_item_ consists of four parts:
      1. The course discipline and catalog number followed by any aliases in parentheses
      2.  Sending/Receiving Flags:
          - For source courses, the minimum grade required (‘P’ means any passing grade)
          - For destination courses, some combination of the letters ‘M’ for Message courses and ‘B’ for blanket credit courses (```-``` if not)
      3. A colon-separated list of codes that tell what requirements the course can satisfy:
          - A two-letter Pathways area (EC, MQ, LP, WG, US, IS, CE, SW, or ```--``` for none)
          - CO for College Option or ```--``` for not
          - ME if this is a major-equivalency course or ```--``` for not
          - *nnn* for the number of majors for which this course can satisfy a requirement

The *mk\_requirement\_dicts* module includes the <code>mk_dicts()</code> method to populate the cuny_courses table’s <code>requirements</code> field with dicts showing what requirements each active course can satisify. This information is used in generating the requirements for the courses that are involved in each rule.
