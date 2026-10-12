# Date patterns: `to_char`, `to_timestamp`, `to_date`, `date_format`, `str_to_date`

These functions format a `date` or `datetime` value as text with a pattern, and read text back to a
value with a pattern. CamusDB accepts two pattern languages:

- **PostgreSQL template patterns**, for example `'YYYY-MM-DD HH24:MI:SS'`.
- **MySQL `%` specifiers**, for example `'%Y-%m-%d %H:%i:%s'`.

Both languages compile to the same fields, so a field formats and parses the same way in both.

| Function | Result | Pattern language |
|----------|--------|------------------|
| `to_char(temporal, pattern)` | `string` | PostgreSQL |
| `to_timestamp(text, pattern)` | `datetime` | PostgreSQL |
| `to_date(text, pattern)` | `date` | PostgreSQL |
| `date_format(temporal, pattern)` | `string` | MySQL |
| `str_to_date(text, pattern)` | `datetime` | MySQL |

Two one-argument forms keep their meaning:

- `to_date(x)` is the cast to `date`, the same as `CAST(x AS date)`.
- `to_timestamp(seconds)` is the PostgreSQL spelling of `from_unixtime(seconds)`.

A `temporal` argument is a `date`, a `datetime`, or an ISO-8601 string with an explicit offset. This
is the same rule as the other date functions. A `NULL` argument gives `NULL`.

```sql
SELECT to_char(created, 'FMDay, DD Mon YYYY HH12:MI AM') FROM events;
-- Sunday, 16 Jun 2024 10:30 AM

INSERT INTO imports (id, day) VALUES (1, to_date('03/15/2026', 'MM/DD/YYYY'));

SELECT date_format(created, '%W %M %e, %Y') FROM events;
-- Sunday June 16, 2024

SELECT * FROM events WHERE created >= str_to_date('June 17, 2024', '%M %e, %Y');
```

## Rules for all patterns

- **All values are UTC.** The time zone fields print `UTC` and `+00`.
- **Names are English only.** Month and day names do not change with the server culture, and there
  is no locale setting.
- **The functions are deterministic.** They are not volatile, so a `CHECK` constraint, a column
  `DEFAULT` and a view body can use them.
- **The year range is 1-9999.** A value outside this range is an error. It does not wrap.
- **The fraction has six digits.** No field prints or reads the seventh fraction digit (100 ns) of a
  `datetime`. `to_char` drops that digit.
- **Each pattern compiles once.** The engine keeps the compiled form of each distinct pattern text in
  a small cache. A literal pattern does not compile again for each row.

## PostgreSQL template patterns

A keyword is case-sensitive, as in PostgreSQL. Each numeric field has an upper-case and a lower-case
spelling (`YYYY` and `yyyy`). Each name field has three spellings, and the spelling sets the case of
the printed name.

| Pattern | Format output | Notes |
|---------|---------------|-------|
| `YYYY` | year, 4 digits (`0099`, `2026`) | |
| `YYY`, `YY`, `Y` | last 3, 2 or 1 digits of the year | Parse: the nearest year to 2020 (see below) |
| `IYYY` | ISO-8601 year, 4 digits | |
| `MM` | month, `01`-`12` | |
| `Mon`, `MON`, `mon` | `Mar`, `MAR`, `mar` | |
| `Month`, `MONTH`, `month` | `March`, `MARCH`, `march`, padded with blanks to 9 characters | |
| `DD` | day of the month, `01`-`31` | |
| `DDD` | day of the year, `001`-`366` | |
| `D` | day of the week, `1` = Sunday ... `7` = Saturday | |
| `ID` | ISO day of the week, `1` = Monday ... `7` = Sunday | |
| `Dy`, `DY`, `dy` | `Sun`, `SUN`, `sun` | |
| `Day`, `DAY`, `day` | `Sunday`, `SUNDAY`, `sunday`, padded with blanks to 9 characters | |
| `WW` | week of the year, `01`-`53`; week 1 is January 1-7 | |
| `IW` | ISO-8601 week, `01`-`53` | |
| `Q` | quarter, `1`-`4` | |
| `HH`, `HH12` | hour, `01`-`12` | |
| `HH24` | hour, `00`-`23` | |
| `MI` | minute, `00`-`59` | |
| `SS` | second, `00`-`59` | |
| `MS` | fraction of the second in milliseconds, `000`-`999` | |
| `US` | fraction of the second in microseconds, `000000`-`999999` | |
| `AM`, `PM`, `am`, `pm` | `AM` or `PM` from the hour; the case follows the pattern | `AM` and `PM` are the same field |
| `TZ`, `tz` | `UTC`, `utc` | |
| `OF` | `+00` | |
| `FM` prefix | turns off the zero and blank padding of the next field only | `FMMM` prints `3`; `FMMonth` prints `March` |
| `"text"` | the text as written | Use `\"` for a double quote |

Any other character prints as written. Put letters in double quotes, because a letter can start a
keyword. For example, write `'YYYY-MM-DD"T"HH24:MI:SS'` for the ISO-8601 form.

## MySQL specifiers

| Specifier | Format output | Notes |
|-----------|---------------|-------|
| `%Y` | year, 4 digits | |
| `%y` | year, 2 digits | Parse: 00-69 is 2000-2069, 70-99 is 1970-1999 |
| `%m` | month, `01`-`12` | |
| `%c` | month, `1`-`12` | |
| `%d` | day of the month, `01`-`31` | |
| `%e` | day of the month, `1`-`31` | |
| `%H` | hour, `00`-`23` | |
| `%h`, `%I` | hour, `01`-`12` | |
| `%i` | minute, `00`-`59` | |
| `%s`, `%S` | second, `00`-`59` | |
| `%f` | fraction of the second in microseconds, `000000`-`999999` | |
| `%p` | `AM` or `PM` | |
| `%M` | month name, `January`-`December` | |
| `%b` | month name, 3 letters, `Jan`-`Dec` | |
| `%W` | day name, `Sunday`-`Saturday` | |
| `%a` | day name, 3 letters, `Sun`-`Sat` | |
| `%j` | day of the year, `001`-`366` | |
| `%u` | week, `00`-`53`; weeks start on Monday, the same as `WEEK(x, 1)` | Days before week 1 are week 0 |
| `%v` | ISO-8601 week, `01`-`53`, the same as `WEEK(x, 3)` | |
| `%%` | `%` | |

Any other specifier is an error. MySQL prints the letter instead, which hides a mistake in the pattern.

## How a parse works

`to_timestamp`, `to_date` and `str_to_date` read the text strictly. Text that does not match the
pattern gives an `InvalidInput` error. The error message quotes the text and the pattern, and it
gives the reason. For example:

```text
Function 'to_date' could not parse '02/30/2026' with pattern 'MM/DD/YYYY': day 30 is out of range for February 2026, which has 28 days
```

These are the rules:

1. A numeric field reads from 1 digit up to its full width. Thus `3/5/2026` matches `MM/DD/YYYY`,
   and `20260315` matches `YYYYMMDD`.
2. A fraction field reads the digits after the decimal point. `MS` reads `5` as 500 ms, and `US`
   or `%f` reads `5` as 500000 µs.
3. A name field matches English names without regard to case. `Mar`, `MAR` and `mar` all match `Mon`.
4. A run of spaces in the pattern matches a run of one or more spaces in the text. Thus the blank
   padding that `Month` and `Day` print reads back.
5. Any other character in the pattern, and quoted text, must match the text exactly.
6. The whole text must match. An extra character after the end of the pattern is an error.
7. Each field must be in range. For example, `MM` is 1-12, `HH24` is 0-23, and `SS` is 0-59.
8. The pattern must have a year field. CamusDB cannot hold a date without a year.
9. A missing month or day is 1. A missing hour, minute, second or fraction is 0.
10. Two fields that give the same value must agree. For example, `MM` and `Mon` must name the
    same month.

The date comes from one of these sources:

- the year, the month and the day;
- the year and `DDD` or `%j` (day of the year), when the pattern has no month and no day;
- `IYYY`, `IW` and `ID` (ISO year, ISO week and ISO day). A missing `IW` is week 1, and a missing
  `ID` is Monday.

A pattern with `IYYY` must not also have a Gregorian date field (`YYYY`, `MM`, `DD`, `DDD`).

Some fields only repeat what the date already tells. The parse checks these fields against the
result: `D`, `ID`, `Dy`, `Day`, `%W`, `%a`, `Q`, `WW`, `IW`, `%u`, `%v`, and `DDD` / `%j` when the
pattern also has a month or a day. A mismatch is an error, because the text then names two
different dates.

The clock fields follow these rules:

- With a 12-hour field (`HH`, `HH12`, `%h`, `%I`), `AM` / `PM` sets the half of the day. Without
  `AM` / `PM`, the hour is AM, so `12` is midnight.
- `AM` / `PM` without a 12-hour field is an error.
- When a pattern has both a 12-hour and a 24-hour field, they must give the same hour.
- When a pattern has both `MS` and `US`, they must agree. `to_char` prints `020` and `020000` for
  20 ms, and that text reads back as 20 ms.
- `TZ` reads `UTC` and `OF` reads `+00`. Any other time zone is an error.

`to_date` reads and checks the time fields, then keeps the date only.

### Two-digit and three-digit years

`YYY`, `YY` and `Y` (and `%y`) give the nearest year to 2020, with the PostgreSQL rule:

| Value read | Year |
|------------|------|
| 0-69 | 2000-2069 |
| 70-99 | 1970-1999 |
| 100-519 | 2100-2519 |
| 520-999 | 1520-1999 |

`YYYY` and `%Y` read the year as written. Thus `26` with `YYYY` is the year 26.

## Result types

`str_to_date` always returns `datetime`. MySQL returns a `date` or a `datetime`, and the pattern
decides which one. The CamusDB result type comes from the argument types only, not from the pattern
text.

CamusDB does not compare a `date` with a `datetime`. To compare the result of `str_to_date` with a
`date` column, cast the result:

```sql
SELECT * FROM events WHERE day = CAST(str_to_date('June 17, 2024', '%M %e, %Y') AS date);
```

You can also use `to_date(text, pattern)`, which returns `date`.

## Differences from PostgreSQL

- **Separators.** PostgreSQL lets any separator in the pattern match any separator in the text,
  and it skips extra spaces. CamusDB requires the same character. Only a run of spaces matches a
  run of spaces of a different length.
- **Quoted text.** PostgreSQL skips the same number of characters without a check. CamusDB requires
  the same text.
- **Extra text.** PostgreSQL ignores some characters after the end of the pattern. CamusDB gives an
  error.
- **Checked fields.** PostgreSQL ignores `D`, `Dy`, `Day` and `Q` in a parse, and it uses `WW` to
  find a date. CamusDB checks each of these fields against the date. `WW` does not set the date.
- **No year.** PostgreSQL gives a date in the year 1 BC when the pattern has no year field.
  CamusDB gives an error, because it cannot hold that year.
- **`MS` and `US` together.** PostgreSQL adds the two values. CamusDB requires them to agree, so the
  text that `to_char` prints reads back.
- **`AM` / `PM` with `HH24`.** PostgreSQL ignores the meridian. CamusDB gives an error.
- **`TZ` and `OF` in a parse.** CamusDB reads only `UTC` and `+00`.
- **Empty pattern.** PostgreSQL `to_char(x, '')` gives `NULL`. CamusDB gives an empty string.
- **Not supported.** Number formatting (`to_char(123, '999')`), the `TH` suffix, `FX`, `TM`, `CC`,
  `J`, `RM`, `A.M.` / `P.M.`, `BC` / `AD`, `TZH` / `TZM`, and `SSSS`.

## Differences from MySQL

- **Unknown specifiers.** MySQL prints the letter. CamusDB gives an error.
- **Strict parse.** MySQL accepts some text that does not match the pattern, and it can give a
  zero date (`0000-00-00`). CamusDB gives an error for each mismatch, and it requires a year.
- **Checked fields.** MySQL does not check `%W`, `%a`, `%u` and `%v` against a year-month-day
  date. CamusDB checks each of them against the date.
- **Result type.** `str_to_date` always returns `datetime`.
- **Names do not pad.** `%M` and `%W` print the name without blanks, as in MySQL.
- **Not supported.** `%D`, `%k`, `%l`, `%r`, `%T`, `%U`, `%V`, `%w`, `%X`, `%x`.
