// Monary - Copyright 2011-2014 David J. C. Beach
// Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

// TODO: If we want C89, we lose inlining. The easiest way to fix this is to
// manually inline everything - it will kill readability but it will make it
// much more portable.

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

// TODO: These are wrong?
#include "mongo.h"
#include "bson.h"

#ifndef NDEBUG
#define DEBUG(format, ...) \
    fprintf(stderr, "[DEBUG] %s:%i " format "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
#define DEBUG(...)
#endif

#define DEFAULT_MONGO_HOST "127.0.0.1"
#define DEFAULT_MONGO_PORT 27017

// TODO: Add BSON array
enum {
    TYPE_UNDEFINED = 0,
    TYPE_OBJECTID = 1,
    TYPE_BOOL = 2,
    TYPE_INT8 = 3,
    TYPE_INT16 = 4,
    TYPE_INT32 = 5,
    TYPE_INT64 = 6,
    TYPE_UINT8 = 7,
    TYPE_UINT16 = 8,
    TYPE_UINT32 = 9,
    TYPE_UINT64 = 10,
    TYPE_FLOAT32 = 11,
    TYPE_FLOAT64 = 12,
    TYPE_DATE = 13,      // BSON date-time, seconds since the UNIX epoch (uint64 storage)
    TYPE_TIMESTAMP = 14, // BSON timestamp (uint64 storage)
    TYPE_STRING = 15,    // each record is (type_arg) chars in length
    TYPE_BINARY = 16,    // each record is (type_arg) bytes in length
    TYPE_BSON = 17,      // get BSON (sub-)document as binary (each record is type_arg bytes)
    TYPE_TYPE = 18,      // BSON type code (uint8 storage)
    TYPE_SIZE = 19,      // data size of a string, symbol, binary, or bson object (uint32)
    TYPE_LENGTH = 20,    // length of string (character count) or num elements in BSON (uint32)
    LAST_TYPE = 20,
};

typedef bson_oid_t OBJECTID;
typedef int64_t DATETIME;
#ifndef WIN32
typedef char BOOL;
typedef char INT8;
#endif
typedef short INT16;
typedef int INT32;
typedef int64_t INT64;
typedef unsigned char UINT8;
typedef unsigned short UINT16;
typedef unsigned int UINT32;
typedef uint64_t UINT64;
typedef float FLOAT32;
typedef double FLOAT64;

/**
 * Makes a new connection to a MongoDB server. It also allows for
 * authentication to a database using a username and password. This combines
 * the old behaviors of monary_connect and monary_authenticate.
 *
 * A new feature is the options parameter, which allows you to specify extra
 * options to the MongoDB connection URI.
 *
 * @param host The name of the host to connect to; if it is NULL, then it
 * uses the default MongoDB hostname.
 * @param port The port to connect to on the host; if the port is 0, then it
 * uses the default MongoDB port number.
 * @param db The database to connect to. If this is not specified but username
 * and password credentials exist, then it defaults to the admin database as
 * per mongoc_uri(7).
 * @param user An optional username.
 * @param pass A password for the user.
 * @param options Additional options for the MongoDB connection URI.
 *
 * @return A pointer to a mongoc_client_t. Note that this object is NOT thread
 * safe and can only be used from one thread at a time. See mongoc_client(3).
 */
mongoc_client_t* monary_connect(const char* host,
                                int port,
                                const char* db,
                                const char* user,
                                const char* pass,
                                const char* options)
{
    // TODO: Don't use asprintf; use malloc and snprintf instead
    // XXX: Draft only. Code is horrific. Make sure it works, then rewrite it
    char *uri, *userpass;

    if(host == NULL) {
        host = DEFAULT_MONGO_HOST;
    }
    if(port == 0) {
        port = DEFAULT_MONGO_PORT;
    }
    if (!db) {
        db = "";
    }
    // TODO: This logic for user/pass is incorrect
    if(user || pass) {
        userpass = asprintf(&userpass, "%s:%s@", user, pass);
    }
    else {
        userpass = (char *) calloc(1, sizeof(char));
    }
    if (!options) {
        options = "";
    }

    asprintf(&uri, "mongodb://%s%s:%i/%s?%s", userpass, host, port, db, options);

    DEBUG("attempting connection to: '%s' port %i", options.host, options.port);

    mongoc_client_t* client = mongoc_client_new(uri);

    if(client) {
        DEBUG("connected successfully");
        return client;
    } else {
        DEBUG("an error occurred when attempting to connect to %s\n", uri);
        return NULL;
    }

    // TODO
    // Cleanup
    free(userpass);
    free(uri);
}

/**
 * Makes a new connection to a MongoDB server and database.
 *
 * @param uri A MongoDB URI, as per mongoc_uri(7).
 *
 * @return A pointer to a mongoc_client_t, or NULL if the connection attempt
 * was unsuccessful.
 */
mongo_client_t *monary_connect_uri(const char *uri) {
    mongo_client_t *client;
    if (!uri) {
        return NULL;
    }

    DEBUG("Attempting connection to: %s", uri);
    client = mongoc_client_new(uri);
    if (client) {
        DEBUG("Connection successful.");
    }
    else {
        DEBUG("An error occurred while attempting to connect to %s\n", uri);
    }
    return client;
}

/**
 * Destroys all resources associated with the client.
 */
void monary_disconnect(mongoc_client_t* client)
{
    mongoc_client_destroy(client)
}

typedef struct monary_column_item monary_column_item;

/**
 * Represents one column item.
 *
 * @memb field The name of the field in the document.
 * @memb type The BSON type identifier, as specified by the Monary type enum.
 * @memb type_arg TODO What is this?
 * @memb storage A pointer to the location of the field data in memory.
 * @memb mask TODO Used for things but not sure what it's for.
 */
struct monary_column_item
{
    char* field;
    unsigned int type;
    unsigned int type_arg;
    void* storage;
    unsigned char* mask;
};

/**
 * Represents a column of items.
 *
 * @memb num_columns The number of items per column.
 * @memb num_rows TODO I have no idea how this plays in
 * @memb columns A pointer to the first item in the column.
 */
typedef struct monary_column_data
{
    unsigned int num_columns;
    unsigned int num_rows;
    monary_column_item* columns;
} monary_column_data;

/**
 * A MongoDB cursor augmented with Monary column data.
 */
typedef struct monary_cursor {
    mongoc_cursor_t* mcursor;
    monary_column_data* coldata;
} monary_cursor;

monary_column_data* monary_alloc_column_data(unsigned int num_columns,
                                             unsigned int num_rows)
{
    // TODO: Check this but LGTM
    if(num_columns > 1024) { return NULL; }
    monary_column_data* result = (monary_column_data*) malloc(sizeof(monary_column_data));
    monary_column_item* columns = (monary_column_item*) calloc(num_columns, sizeof(monary_column_item));
    result->num_columns = num_columns;
    result->num_rows = num_rows;
    result->columns = columns;

    return result;
}

int monary_free_column_data(monary_column_data* coldata)
{
    // Only changes if monary_alloc_column_data changes
    if(coldata == NULL || coldata->columns == NULL) { return 0; }
    for(int i = 0; i < coldata->num_columns; i++) {
        monary_column_item* col = coldata->columns + i;
        if(col->field != NULL) { free(col->field); }
    }
    free(coldata->columns);
    free(coldata);
    return 1;
}

/**
 * Sets the field value for a particular column and item.
 *
 * @param coldata A pointer to the column data to modify.
 * @param colnum The number of the item to modify. Must be within range for
 * coldata.
 * @param field The new name of the column item.
 * @param type The new type of the item.
 * @param type_arg TODO
 * @param storage A pointer to the new location of the data in memory. Note
 * that this does not free(3) the previous storage pointer.
 * @param mask TODO
 *
 * @return 1 if the modification was performed successfully; 0 otherwise.
 */
int monary_set_column_item(monary_column_data* coldata,
                           unsigned int colnum,
                           const char* field,
                           unsigned int type,
                           unsigned int type_arg,
                           void* storage,
                           unsigned char* mask)
{
    if(coldata == NULL) { return 0; }
    if(colnum >= coldata->num_columns) { return 0; }
    if(type == TYPE_UNDEFINED || type > LAST_TYPE) { return 0; }
    if(storage == NULL) { return 0; }
    if(mask == NULL) { return 0; }
    
    int len = strlen(field);
    if(len > 1024) { return 0; }
    
    monary_column_item* col = coldata->columns + colnum;

    col->field = (char*) malloc(len + 1);
    strcpy(col->field, field);
    
    col->type = type;
    col->type_arg = type_arg;
    col->storage = storage;
    col->mask = mask;

    return 1;
}

inline int monary_load_objectid_value(const bson_iter_t* bsonit,
                                      monary_column_item* citem,
                                      int idx)
{
    if (BSON_ITER_HOLDS_OID) {
        const OBJECTID* oid = bson_iter_oid(bsonit);
        OBJECTID* oidloc = ((OBJECTID*) citem->storage) + idx;

        // Move over all 12 bytes. Faster than memcpy(3)?
        int i;
        for (i = 0; i < 12; i++) {
            oidloc->bytes[i] = oid->bytes[i];
        }
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_bool_value(bson_iter_t* bsonit,
                                  bson_type_t type,
                                  monary_column_item* citem,
                                  int idx)
{
    BOOL value = bson_iter_bool(bsonit);
    ((BOOL*) citem->storage)[idx] = value;
    return 1;
}

// TODO Check to make sure we're doing the right thing re: unsigned num types??
// TODO if we change the number of supported types, it will be better to write
// them out instead of keeping them as macros so we can take advantage of the
// built-in BSON type-checking macros
#define MONARY_DEFINE_NUM_LOADER(FUNCNAME, NUMTYPE, BSONTYPE, BSONFUNC) \
inline int FUNCNAME (const bson_iter_t* bsonit,                         \
                     bson_type_t type,                                  \
                     monary_column_item* citem,                         \
                     int idx)                                           \
{                                                                       \
    if(type = BSONTYPE) {                                               \
        NUMTYPE value = BSONFUNC(bsonit);                               \
        ((NUMTYPE*)citem->storage)[idx] = value;                        \
        return 1;                                                       \
    } else {                                                            \
        return 0;                                                       \
    }                                                                   \
}

// XXX An example:
inline int monary_load

MONARY_DEFINE_NUM_LOADER(monary_load_int8_value, INT8, bson_iter_int)
MONARY_DEFINE_NUM_LOADER(monary_load_int16_value, INT16, bson_iter_int)
MONARY_DEFINE_NUM_LOADER(monary_load_int32_value, INT32, bson_iter_int)
MONARY_DEFINE_NUM_LOADER(monary_load_int64_value, INT64, bson_iter_long)
MONARY_DEFINE_NUM_LOADER(monary_load_uint8_value, UINT8, bson_iter_int)
MONARY_DEFINE_NUM_LOADER(monary_load_uint16_value, UINT16, bson_iter_int)
MONARY_DEFINE_NUM_LOADER(monary_load_uint32_value, UINT32, bson_iter_int)
MONARY_DEFINE_NUM_LOADER(monary_load_uint64_value, UINT64, bson_iter_long)
MONARY_DEFINE_NUM_LOADER(monary_load_float32_value, FLOAT32, bson_iter_double)
MONARY_DEFINE_NUM_LOADER(monary_load_float64_value, FLOAT64, bson_iter_double)

inline int monary_load_datetime_value(const bson_iter_t* bsonit,
                                      monary_column_item* citem,
                                      int idx)
{
    if (BSON_ITER_HOLDS_DATE_TIME(bsonit)) {
        DATETIME value = bson_iter_date_time(bsonit);
        ((DATETIME*) citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

// TODO Support or no?
inline int monary_load_timestamp_value(const bson_iter_t* bsonit,
                                       bson_type_t type,
                                       monary_column_item* citem,
                                       int idx)
{
    if(type == BSON_TYPE_TIMESTAMP) {
        bson_timestamp_t value = bson_iter_timestamp(bsonit);
        ((UINT64*) citem->storage)[idx] = *((INT64*) &value);
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_string_value(const bson_iter_t* bsonit,
                                    monary_column_item* citem,
                                    int idx)
{
    // TODO Ask Jason or a C person about UTF-8 in C
    if (BSON_ITER_HOLDS_UTF8(bsonit)) {
        unsigned int size = citem->type_arg;
        const char* src = bson_iter_utf8(bsonit, size);
        char* dest = ((char*) citem->storage) + (idx * size);
        strncpy(dest, src, size);
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_binary_value(bson_iter_t* bsonit,
                                    monary_column_item* citem,
                                    int idx)
{
    if (BSON_ITER_HOLDS_BINARY(iter)) {
        // TODO This needs to use the new bson_iter_binary(3) function, which
        // gives you all the data you could possibly want.
        int size = citem->type_arg;
        int binlen = bson_iter_bin_len(bsonit);
        if(binlen > size) { binlen = size; }
        const char* src = bson_iter_bin_data(bsonit);
        char* dest = ((char*) citem->storage) + (idx * size);
        memcpy(dest, src, binlen);
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_bson_value(bson_iter_t* bsonit,
                                  bson_type_t type,
                                  monary_column_item* citem,
                                  int idx)
{
    // TODO
    if(type == bson_object || type == bson_array) {
        bson subobj;
        bson_iter_subobject(bsonit, &subobj);
        int size = citem->type_arg;
        int binlen = bson_size(&subobj);
        if(binlen > size) { binlen = size; }
        char* src = subobj.data;
        char* dest = ((char*) citem->storage) + (idx * size);
        memcpy(dest, src, binlen);
        return 1;
    } else {
        return 0;
    }
}

inline int monary_load_type_value(bson_iter_t* bsonit,
                                  bson_type_t type,
                                  monary_column_item* citem,
                                  int idx)
{
    ((UINT8*) citem->storage)[idx] = type;
    return 1;
}

inline int monary_load_size_value(bson_iter_t* bsonit,
                                  bson_type_t type,
                                  monary_column_item* citem,
                                  int idx)
{
    UINT32 size = 0;
    // TODO Changed to use the new type enum
    if(type == BSON_TYPE_UTF8 || type == BSON_TYPE_CODE || type == BSON_TYPE_SYMBOL) {
        // NOTE: Binary size of string includes terminating '\0' character.
        size = bson_iter_string_len(bsonit);
    } else if(type == BSON_TYPE_BINARY) {
        size = bson_iter_bin_len(bsonit);
    } else if(type == BSON_TYPE_ARRAY || type == bson_object) { // TODO What the hell is "bson_object"
        // TODO Does this mean we have array support?
        // TODO This is very wrong <<XXX
        bson subobj;
        bson_iter_t_subobject(bsonit, &subobj);
        size = bson_size(&subobj);
        // XXX
    } else {
        return 0;
    }
    ((UINT32*) citem->storage)[idx] = size;
    return 1;
}

inline int monary_load_length_value(bson_iter_t* bsonit,
                                    bson_type_t type,
                                    monary_column_item* citem,
                                    int idx)
{
    UINT32 length = 0;
    if(type == bson_string || type == bson_code || type == bson_symbol) {
        // The length of the string is the character count.  Since the string
        // is UTF-8 encoded, we must count characters appropriately...
        const char* s = bson_iter_t_string(bsonit);
        length = mbstowcs(NULL, s, 0);
    } else if(type == bson_array || type == bson_object) {
        // The length of a BSON object is the number of elements (i.e. key-value pairs)
        // it contains.  We count these by iterating over the elements.
        bson_iter_t subiter;
        bson_iter_t_subiterator(bsonit, &subiter);
        while(bson_iter_t_next(&subiter) != 0) { ++length; }
    } else {
        // Other objects are not considered to have a length.
        return 0;
    }
    ((UINT32*) citem->storage)[idx] = length;
    return 1;
}

#define MONARY_DISPATCH_TYPE(TYPENAME, TYPEFUNC)    \
case TYPENAME:                                      \
success = TYPEFUNC(bsonit, type, citem, offset);    \
break;

int monary_load_item(bson_iter_t* bsonit,
                     bson_type_t type,
                     monary_column_item* citem,
                     int offset)
{
    int success = 0;

    switch(citem->type) {
        MONARY_DISPATCH_TYPE(TYPE_OBJECTID, monary_load_objectid_value)
        MONARY_DISPATCH_TYPE(TYPE_BOOL, monary_load_bool_value)

        MONARY_DISPATCH_TYPE(TYPE_INT8, monary_load_int8_value)
        MONARY_DISPATCH_TYPE(TYPE_INT16, monary_load_int16_value)
        MONARY_DISPATCH_TYPE(TYPE_INT32, monary_load_int32_value)
        MONARY_DISPATCH_TYPE(TYPE_INT64, monary_load_int64_value)

        MONARY_DISPATCH_TYPE(TYPE_UINT8, monary_load_uint8_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT16, monary_load_uint16_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT32, monary_load_uint32_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT64, monary_load_uint64_value)

        MONARY_DISPATCH_TYPE(TYPE_FLOAT32, monary_load_float32_value)
        MONARY_DISPATCH_TYPE(TYPE_FLOAT64, monary_load_float64_value)

        MONARY_DISPATCH_TYPE(TYPE_DATE, monary_load_datetime_value)
        MONARY_DISPATCH_TYPE(TYPE_TIMESTAMP, monary_load_timestamp_value)

        MONARY_DISPATCH_TYPE(TYPE_STRING, monary_load_string_value)
        MONARY_DISPATCH_TYPE(TYPE_BINARY, monary_load_binary_value)
        MONARY_DISPATCH_TYPE(TYPE_BSON, monary_load_bson_value)

        MONARY_DISPATCH_TYPE(TYPE_TYPE, monary_load_type_value)
        MONARY_DISPATCH_TYPE(TYPE_SIZE, monary_load_size_value)
        MONARY_DISPATCH_TYPE(TYPE_LENGTH, monary_load_length_value)
    }

    return success;
}

/**
 * Copies over raw BSON data into Monary column storage. This function
 * determines the types of the data, dispatches to an appropriate handler and
 * copies over the data. It keeps a count of any unsuccessful loads and sets
 * NumPy-compatible masks on the data as appropriate.
 *
 * @param coldata A pointer to monary_column_data which contains the final
 * storage location for the BSON data.
 * @param row The row number to store the data in. Cannot exceed
 * coldata->num_columns.
 * @param bson_data A pointer to the raw BSON data to load.
 *
 * @return The number of unsuccessful loads.
 */
int monary_bson_to_arrays(monary_column_data* coldata,
                          unsigned int row,
                          const bson_t* bson_data)
{
    bson_type_t found_type;
    int num_masked = 0;
    bson_iter_t bsonit;

    for(int i = 0; i < coldata->num_columns; i++) {
        int offset = row;
        int success = 0;
        monary_column_item* citem = coldata->columns + i;
        
        if (bson_iter_init_find(&bsonit, bson_data, citem->field)) {
            // Determine the BSON type of the observed item on the iterator
            found_type = bson_iter_type(&bsonit);

            // Dispatch to appropriate column handler (inlined)
            if (found_type) {
                success = monary_load_item(&bsonit, found_type, citem, offset);
            }
        }

        // Record success result in mask, if applicable
        if (citem->mask != NULL) {
            citem->mask[offset] = !success;
        }

        // tally number of masked (unsuccessful) loads
        if(!success) { ++num_masked; }
    }
    
    return num_masked;
}

/**
 * Performs a count query on a MongoDB collection.
 * TODO
 *
 * @return The number of documents in the collection matching the query.
 */
long monary_query_count(mongo_connection* connection,
                        const char* db_name,
                        const char* coll_name,
                        const char* query)
{

    // build BSON query data
    bson query_bson;
    bson_init(&query_bson, (char*) query, 0);
    
    long total_count = mongo_count(connection, db_name, coll_name, &query_bson);
    
    bson_destroy(&query_bson);

    return total_count;
}

void monary_get_bson_fields_list(monary_column_data* coldata,
                                 bson* fields_bson)
{
    bson_buffer fields_builder;
    bson_buffer_init(&fields_builder);
    for(int i = 0; i < coldata->num_columns; i++) {
        monary_column_item* col = coldata->columns + i;
        bson_append_int(&fields_builder, col->field, 1);
    }
    bson_from_buffer(fields_bson, &fields_builder);
}

/**
 * Performs a find query on a MongoDB collection, selecting certain fields from
 * the results and storing them in Monary columns.
 *
 * @param collection The MongoDB collection to query against.
 * @param flags Flags for the query.
 * @param skip The number of documents to skip, or zero.
 * @param limit The maximum number of documents to return, or zero.
 * @param batch_size The batch size of document results sets, or zero for the
 * default value (which is 100).
 * @param query A valid JSON-compatible UTF-8 string query. This function does
 * no validation, so you should ensure that the query is well-formatted.
 * @param fields A JSON-compatible UTF-8 string containing fields to return, or
 * NULL.
 * @param read_prefs Read preferences, or NULL for the default preferences.
 * @param coldata The column data to store the results in.
 * @param select_fields If truthy, select exactly the fields from the database
 * that match the fields in coldata. To disable this, set this to false.
 * TODO Can we even use these flags? Do we care? How does this play with the Python?
 *
 * @return A Monary cursor that should be freed with monary_close_query(3) when
 * no longer in use.
 */
monary_cursor* monary_init_query(mongoc_collection_t *collection,
                                 mongoc_query_flags_t flags,
                                 uint32_t skip,
                                 uint32_t limit,
                                 uint32_t batch_size,
                                 const uint8_t *data,
                                 const uint8_t *fields,
                                 const mongoc_read_prefs_t *read_prefs,
                                 monary_column_data *coldata,
                                 int select_fields)
{
    // build BSON query data
    bson query_bson;
    bson_init(&query_bson, (char*) query, 0);

    // build BSON fields list (if necessary)
    bson query_fields;
    if(select_fields) { monary_get_bson_fields_list(coldata, &query_fields); }

    // create query cursor
    bson* fields_ptr = select_fields ? &query_fields : NULL;
    mongo_cursor* mcursor = mongo_find(connection, ns, &query_bson, fields_ptr, limit, offset, 0);

    // destroy BSON fields
    bson_destroy(&query_bson);
    if(select_fields) { bson_destroy(&query_fields); }

    monary_cursor* cursor = (monary_cursor*) malloc(sizeof(monary_cursor));
    cursor->mcursor = mcursor;
    cursor->coldata = coldata;
    
    return cursor;
}

/**
 * Grabs the results obtained from the MongoDB cursor and loads them into
 * in-memory arrays.
 *
 * @param cursor A pointer to a Monary cursor, which contains both a MongoDB
 * cursor and Monary column data that stores the retrieved information.
 *
 * @return The number of rows loaded into memory.
 */
int monary_load_query(monary_cursor* cursor)
{
    bson_error_t error;
    const bson_t *bson;
    int num_masked;
    int row;
    monary_column_data *coldata;
    mongoc_cursor_t *mcursor;

    mcursor = cursor->mcursor;
    coldata = cursor->coldata;
    row = 0;
    num_masked = 0;
    
    // read result values
    while(row < coldata->num_rows && mongoc_cursor_next(mcursor, &bson)) {

#ifndef NDEBUG
        if(row % 500000 == 0) {
            DEBUG("...%i rows loaded", row);
        }
#endif

        num_masked += monary_bson_to_arrays(coldata, row, &(mcursor->current));
        ++row;
    }

    // TODO: Put the error handling here, or earlier? (If earlier, it has to be
    // in Python code.) There are try-excepts where this is invoked in
    // Monary.query in the Python, so perhaps the errors may never propagate
    // here?
    // Will a python try-except even work on C code??
    if (mongoc_cursor_error(mcursor, &error)) {
        DEBUG("error: %d.%d %s", error.domain, error.code, error.message);
    }

    int total_values = row * coldata->num_columns;
    DEBUG("%i rows loaded; %i / %i values were masked", row, num_masked, total_values);

    bson_destroy(&bson); // TODO
    return row;
}

/**
 * Destroys the underlying MongoDB cursor associated with the given cursor.
 *
 * Note that the column data is not freed in this function as that data is
 * exposed as NumPy arrays in Python.
 *
 * @param cursor A pointer to the Monary cursor to close. If cursor is NULL,
 * no operation is performed.
 */
void monary_close_query(monary_cursor* cursor)
{
    if (cursor) {
        mongoc_cursor_destroy(cursor->mcursor);
        free(cursor);
    }
}
