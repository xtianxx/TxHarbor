# PG18 (18.6) verified ownership semantics — grounded on live runs + docs

Docs cross-checked (retrieved 2026-10-04):
- ddl-priv.html: "Superusers can always do this; ordinary roles can only do it if
  they are both the current owner of the object (or inherit the privileges of the
  owning role) and able to SET ROLE to the new owning role."
- sql-altertable.html: "To alter the owner, you must be able to SET ROLE to the
  new owning role, and that role must have CREATE privilege on the table's
  schema."
- sql-altersequence.html: same wording with "sequence's schema".
- sql-grant.html: membership chain rule "If a role is an indirect member of
  another role, it can use SET ROLE to change to that role only if there is a
  chain of grants each of which has SET TRUE."
- app-pgrestore.html: default archive issues "ALTER OWNER or SET SESSION
  AUTHORIZATION statements"; --role issues "SET ROLE rolename" after connect.

Live run observations (docker postgres:18.6-trixie, digest
sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722,
2026-10-04; raw honor files in this directory):
1. Pre-granting object ownership to a restricted recovery role does NOT by
   itself let the archive's `ALTER TABLE ... OWNER TO writer_owner` succeed:
   the restoring role must be able to SET ROLE to writer_owner (SET TRUE chain)
   AND hold CREATE on the schema (cases 2/3/6/8 vs 1).
2. ALTER ... OWNER TO only succeeds either (a) via membership WITH SET TRUE
   (cases 12 with --role=writer_owner, and case 11 semantics: objects created
   under R stay R-owned when --no-owner suppresses the OWNERSHIP statements) or
   (b) with session already as the owning role.
3. pg_restore --role=R2 issues "SET ROLE R2" after connect: authenticating as
   the restricted role but *acting as* W is a possible legal vector, not used
   here (production planned path A-1 keeps authenticated session = acting role
   = recovery role; the --role vector is a separate decision item).
4. Original-writer W password is irrelevant to these statements; role-member
   grants are the authority. No superuser path was used in any successful case.
