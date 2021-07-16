CREATE FUNCTION pg_catalog.citus_st_asgeobuf_agg_transfn(internal, bytea)
	RETURNS internal
    LANGUAGE C
	AS 'MODULE_PATHNAME', $$citus_st_asgeobuf_agg_transfn$$;

CREATE FUNCTION pg_catalog.citus_st_asgeobuf_agg_finalfn(internal)
	RETURNS bytea
	LANGUAGE C
    AS 'MODULE_PATHNAME', $$citus_st_asgeobuf_agg_finalfn$$;

CREATE AGGREGATE pg_catalog.st_asgeobuf_agg(bytea) (
    SFUNC = citus_st_asgeobuf_agg_transfn,
    FINALFUNC = citus_st_asgeobuf_agg_finalfn,
    STYPE = internal
);
COMMENT ON AGGREGATE pg_catalog.st_asgeobuf_agg(bytea)
    IS 'concatenate input geobuf data into a single geobuf';