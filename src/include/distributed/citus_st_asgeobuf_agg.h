#ifndef CITUS_ST_ASGEOBUF_AGG_H
#define CITUS_ST_ASGEOBUF_AGG_H


#include "postgres.h"
#include "fmgr.h"
#include "geobuf.pb-c.h"

struct citus_geobuf_agg_context {
    uint8_t *buf;
    size_t buf_len;
    uint32_t agg_array_size;
    uint32_t precision;
    uint32_t dimensions;
    uint32_t keys_real_size;
    Geobuf__Data *data;
};
typedef struct citus_geobuf_agg_context citus_geobuf_agg_context;


void citus_geobuf_agg_context_merge_geobuf(struct citus_geobuf_agg_context *ctx, Geobuf__Data *data);
Datum citus_st_asgeobuf_agg_transfn(PG_FUNCTION_ARGS);
Datum citus_st_asgeobuf_agg_finalfn(PG_FUNCTION_ARGS);

size_t geobuf__data__feature__get_packed_size (const Geobuf__Data__Feature *message);
size_t geobuf__data__feature__pack(const Geobuf__Data__Feature *message,uint8_t *out);
Geobuf__Data__Feature *geobuf__data__feature__unpack(ProtobufCAllocator *allocator,size_t len,const uint8_t *data);
Geobuf__Data__Feature * geobuf__data__feature__copy(Geobuf__Data__Feature *src, ProtobufCAllocator *allocator);
void geobuf__data__geometry__change_precision(Geobuf__Data__Geometry *geometry, int factor);
void citus_geobuf_agg_context_add_data(struct citus_geobuf_agg_context *ctx, uint8_t *data_ptr, size_t data_len);
void citus_geobuf_agg_context_merge_geobuf(struct citus_geobuf_agg_context *ctx, Geobuf__Data *data);
void citus_geobuf_agg_context_init(citus_geobuf_agg_context *ctx);
void citus_geobuf_agg_context_clean(citus_geobuf_agg_context *ctx);


#endif