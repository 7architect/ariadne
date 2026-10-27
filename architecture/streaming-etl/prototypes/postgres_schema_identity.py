"""Stable schema identity excluding object OIDs, owners, ACLs and live counters."""

from __future__ import annotations

from postgres_restore_contracts import digest


def _json_value(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


async def schema_identity(conn, schema: str) -> dict:
    # All names are parameters; pg_get_* renders a stable definition on PG17.
    queries = {
        "relations": "SELECT c.relname,c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind IN('r','p','v','m','S') ORDER BY c.relname",
        "columns": "SELECT c.relname,a.attname,a.attnum,format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull,a.attidentity,a.attgenerated,pg_get_expr(d.adbin,d.adrelid) AS default_expr FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE n.nspname=$1 AND a.attnum>0 AND NOT a.attisdropped AND c.relkind IN('r','p','v','m') ORDER BY c.relname,a.attnum",
        "constraints": "SELECT c.relname,k.conname,k.contype,pg_get_constraintdef(k.oid,true) AS definition FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 ORDER BY c.relname,k.conname",
        "indexes": "SELECT t.relname AS table_name,c.relname,pg_get_indexdef(c.oid) AS definition FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname=$1 ORDER BY t.relname,c.relname",
        "triggers": "SELECT c.relname,t.tgname,t.tgenabled,pg_get_triggerdef(t.oid,true) AS definition FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND NOT t.tgisinternal ORDER BY c.relname,t.tgname",
        "functions": "SELECT p.proname,pg_get_function_identity_arguments(p.oid) AS arguments,pg_get_functiondef(p.oid) AS definition FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$1 AND p.prokind IN('f','p') ORDER BY p.proname,pg_get_function_identity_arguments(p.oid)",
        "sequences": "SELECT sequencename,data_type::text,start_value,min_value,max_value,increment_by,cycle,cache_size FROM pg_sequences WHERE schemaname=$1 ORDER BY sequencename",
        "views": "SELECT c.relname,pg_get_viewdef(c.oid,true) AS definition FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind IN('v','m') ORDER BY c.relname",
        "policies": "SELECT p.tablename,p.policyname,p.permissive,p.roles::text[],p.cmd,p.qual,p.with_check FROM pg_policies p WHERE p.schemaname=$1 ORDER BY p.tablename,p.policyname",
        "types": "SELECT t.typname,t.typtype,format_type(t.typbasetype,t.typtypmod) AS base_type,t.typnotnull,t.typdefault,ARRAY(SELECT e.enumlabel FROM pg_enum e WHERE e.enumtypid=t.oid ORDER BY e.enumsortorder) AS labels FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname=$1 AND t.typtype IN('e','d') ORDER BY t.typname",
    }
    identity = {
        name: [_json_value(dict(row)) for row in await conn.fetch(sql, schema)]
        for name, sql in queries.items()
    }
    return {"sha256": digest(identity), "catalog": identity}
