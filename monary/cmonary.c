#include <Python/Python.h>
#include <mongoc.h>
#include <bson.h>
#include "cmonary.h"

/*
 *-------------------------------------------------------------------------
 * cmonary_dealloc --
 *
 *      Frees all data associated with this object.
 *
 *  Parameters:
 *      @self: A pointer to a cmonary_t.
 *
 *  Returns:
 *      Void.
 *
 *  Side effects:
 *      Frees structure member data.
 *-------------------------------------------------------------------------
 */
static void
cmonary_dealloc (cmonary_t *self)
{
    PyListObject *sublist;
    Py_ssize_t    i;
    Py_ssize_t    length;
    int list_idx;

    mongoc_client_destroy(self->client);
    self->ob_type->tp_free((PyObject *) self);
}

/*
 *-------------------------------------------------------------------------
 * cmonary_dealloc_columns --
 *
 *      Frees data owned by a cmonary_column_t allocated on the stack.
 *
 *  Returns:
 *      None.
 *-------------------------------------------------------------------------
 */
static void
cmonary_dealloc_columns (cmonary_column_t *column)
{
    Py_ssize_t length;

    length = PyList_GetSize(column->columns);
    for (--length; length >= 0; length--) {
        Py_DECREF(PyList_GetItem((PyObject *) column->columns, length));
        free(fieldnames + length);
    }
}

/*
 *-------------------------------------------------------------------------
 * cmonary_new --
 *
 *      Creates a new Monary object.
 *
 *  Returns:
 *      A pointer to a newly-allocated object, or NULL in the event of failure.
 *
 *  Side effects:
 *      Allocates memory; in the event of failure, a Python exception is raised.
 *-------------------------------------------------------------------------
 */
static PyObject *
cmonary_new (PyTypeObject *type,
             PyObject     *args)
{
    cmonary_t *self;
    const char *uri;

    if (!PyArg_ParseTuple(args, "s", &uri)) {
        return NULL;
    }

    self = (cmonary_t *) type->tp_alloc(type, 0);
    if (self) {
        self->client = NULL;
    }

    return (PyObject *) self;
}

/*
 *-------------------------------------------------------------------------
 * cmonary_init --
 *
 *      Initializes a new Monary object. (This is the __init__ constructor.)
 *
 *  Returns:
 *      0 if the initialization succeeded; -1 in the event of failure.
 *-------------------------------------------------------------------------
 */
static int
cmonary_init (cmonary_t *self,
              PyObject  *args,
              PyObject  *kwargs)
{
    const char *uri;

    if (!PyArg_ParseTuple("s", &uri)) {
        return -1;
    }

    self->client = mongoc_client_new(uri);
    if (!self->client) {
        Py_DECREF(self);
        PyErr_SetString(PyExc_RuntimeError,
                        "Failed to create a new MongoDB client.");
        return -1;
    }

    return 0;
}

/*
 *-------------------------------------------------------------------------
 * cmonary_collection_count --
 *
 *      Performs a count query on a MongoDB collection.
 *
 * Python Parameters:
 *      @db: The name of the database to access.
 *      @coll: The name of the collection to access.
 *      @query: A Python dictionary representing the query.
 *
 * Returns:
 *      An integer indicating the number of results in the database that match
 *      the given query. If an error occurs, -1 is returned.
 *  
 * Side effects:
 *      This performs a count query on MongoDB so network traffic costs are
 *      incurred.
 *-------------------------------------------------------------------------
 */
static PyObject *
cmonary_collection_count (cmonary_t *self)
{
    PyObject      *query_dict;
    bson_t         query_bson;
    const char    *coll;
    const char    *db;
    const uint8_t *query;
    int32_t        query_size;
    
    if (!PyArg_ParseTuple(args,
                          "ssO!",
                          &db,
                          &coll,
                          &PyDict_Type,
                          &query_dict)) {
        return -1;
    }

    // Sanity checks
    if (!db) {
        PyErr_SetString(PyExc_ValueError, "db name cannot be empty");
        return -1;
    }
    else if (!coll) {
        PyErr_SetString(PyExc_ValueError, "collection name cannot be empty");
        return -1;
    }

    // Convert dictionary into raw bytes
    // TODO

    // Build BSON query data
}

/*
 *-------------------------------------------------------------------------
 * cmonary_collection_find_demo --
 *
 *      Performs a demo query.
 *-------------------------------------------------------------------------
 */
static PyObject *
cmonary_collection_find_demo (cmonary_t *self)
{
    PyObject            *result;
    bson_t               query_bson;
    mongoc_collection_t *collection;
    mongoc_cursor_t     *cursor;
    uint8_t             *query;
    int                  masked;

    query = "\x05\x00\x00\x00\x00";
    collection = mongoc_collection_new("monary", "test");
    if (!collection) {
        PyErr_SetString(PyExc_RuntimeError, "failed to connect to monary.test");
        return NULL;
    }

    if (!bson_init_static(&query_bson, query, 5)) {
        PyErr_SetString(PyExc_RuntimeError, "failed to init bson_t");
        return NULL;
    }

    cursor = mongoc_collection_find(collection,
                                    MONGOC_QUERY_NONE,
                                    0,
                                    0,
                                    0,
                                    &query_bson,
                                    NULL,
                                    NULL);
    bson_destroy(query_bson);
    if (!cursor) {
        PyErr_SetString(PyExc_RuntimeError, "an error occurred with the query");
        return NULL;
    }

    result = PyList_New(0);
    masked = cmonary_load_cursor_demo(cursor, result);
    return result;
}

static int cmonary_load_cursor_demo (mongoc_cursor_t *cursor, /* IN */
                                      PyObject        *list)   /* IN-OUT */
{
    PyObject *obj;
    bson_error_t err;
    bson_iter_t iter;
    const bson_t *bson;
    int masked;
    int32_t value;

    masked = 0;

    while (!mongoc_cursor_error(cursor, &err) && mongoc_cursor_more(cursor)) {
        if (!mongoc_cursor_next(cursor, &bson)) {
        }

        if (!bson_iter_init(&iter, &bson)) {
            return -1;
        }

        if (strcmp("a", bson_iter_key(iter)) == 0) {
            if (BSON_ITER_HOLDS_INT32(iter)) {
                value = bson_iter_int32(iter);
                obj = PyInt_FromLong(value);
                if (PyList_Append(list, obj) == -1) {
                    return -1;
                }
            }
            else {
                masked++;
            }
        }
    }

    if (mongoc_cursor_error(mcursor, &err)) {
        fprintf(stderr, "error: %d.%d %s", err.domain, err.code, err.message);
    }
    return masked;
}

/*
 *-------------------------------------------------------------------------
 * _cmonary_pylist_contains_string --
 *
 *      Given a Python list, determines if it contains the given string.
 *
 *  Parameters:
 *      @list: A Python list.
 *      @value: A C string.
 *
 *  Returns:
 *      True if the list contains the given string; false otherwise. If the
 *      given string is NULL, this always returns false.
 *
 *  Side effects:
 *      None.
 *-------------------------------------------------------------------------
 */
int
_cmonary_pylist_contains_string (PyListObject *list,
                                 char         *value)
{
    PyStringObject *str;
    Py_ssize_t      index;
    Py_ssize_t      size;

    if (!value) {
        return 0;
    }

    size = PyList_Size(list);
    for (index = 0; index < size; index++) {
        str = (PyStringObject) PyList_GetItem(list);
        if (strcmp(PyString_AsString(str), value)) {
            return 1;
        }
    }

    return 0;
}

/*
 *-------------------------------------------------------------------------
 * _cmonary_load_cursor --
 *
 *      Given a cursor, loads the results into the arrays.
 *
 *  Parameters:
 *      @lists: a Monary list containing a list of Python lists and the name of
 *              the field it represents.
 *      @cursor: a MongoDB cursor from libmongoc.
 *
 *  Returns:
 *      The total number of failed loads.
 *-------------------------------------------------------------------------
 */
static int
_cmonary_load_cursor (cmonary_list_t     *lists,  /* IN-OUT */
                      const PyListObject *fields, /* IN */
                      mongoc_cursor_t    *cursor) /* IN */
{
    bson_iter_t iter;
    bson_error_t err;
    const bson_t *bson;
    int masked;
    int row;

    while (!mongoc_cursor_error(cursor, &err) && mongoc_cursor_more(cursor)) {
        if (mongoc_cursor_next(cursor, &bson)) {
            if (!bson_iter_init(&iter, &bson)) {
                return -1;
            }

            
        }
        row++;
    }
}

/*
 *-------------------------------------------------------------------------
 * _cmonary_load_cursor_single --
 *
 *      Given a cursor, loads the results into the arrays.
 *
 *  Parameters:
 *      @lists: a pointer to a list of Python lists.
 *      @field: the single field to load.
 *      @cursor: a MongoDB cursor from libmongoc.
 *
 *  Returns:
 *      The total number of failed loads.
 *-------------------------------------------------------------------------
 */
int
_cmonary_load_cursor_single (cmonary_list_t  *lists,  /* IN-OUT */
                             const char      *field,  /* IN */
                             mongoc_cursor_t *cursor) /* IN */
{
    bson-iter_t iter;
    bson_error_t err;
    const bson_t *bson;
    const char *fieldname;
    int masked;
    int row;
    int success;
    long failures;

    while (!mongoc_cursor_error(cursor, &err) && mongoc_cursor_more(cursor)) {
        if (mongoc_cursor_next(cursor, &bson)) {
            if (!bson_iter_init(&iter, &bson)) {
                return -1;
            }

            success = 0;
            field = bson_iter_key(iter);
            
            if (strcmp(field, fieldname) == 0) {
                success = _cmonary_load_item(&iter);
            }
        }
        row++;
    }
}

/*
 *-------------------------------------------------------------------------
 * _cmonary_load_item --
 *
 *      Loads a value from a MongoDB cursor into a Python list.
 *
 * Parameters:
 *      @iter: A pointer to a bson_iter_t.
 *      @list: A Monary list containing a Python list and the name of the field
 *      it represents.
 *
 *  Returns:
 *      True if the load was successful; false otherwise.
 *
 *  Side effects:
 *      At most one element is consumed from the iterator.
 *-------------------------------------------------------------------------
 */
static int
_cmonary_load_item (bson_iter_t    *iter,
                    cmonary_list_t *list)
{
    double    double_val;
    int32_t   int32_val;
    int64_t   int64_val;
    PyObject *item;

    // Dispatch
    switch (bson_iter_type(iter)) {
        case BSON_TYPE_DOUBLE:
            double_val bson_iter_double(iter);
            item = PyFloat_FromDouble(double_val);
            break;
        case BSON_TYPE_INT32:
            int32_val = bson_iter_int32(iter);
            item = PyInt_FromLong(int32_val);
            break;
        case BSON_TYPE_INT64:
            int64_val = bson_iter_int64(iter);
            item = PyInt_FromLong(int64_val);
            break;
        case BSON_TYPE_UTF8:
        case BSON_TYPE_DOCUMENT:
        case BSON_TYPE_ARRAY:
        case BSON_TYPE_BINARY:
        case BSON_TYPE_OID:
        case BSON_TYPE_BOOL:
        case BSON_TYPE_DATE_TIME:
        default:
            return 0;
    }

    // PyList_Append returns 0 on success and -1 on failure.
    // +1 to convert it to true on success; false otherwise.
    return PyList_Append(list->list, item) + 1;
}

/*
 *-------------------------------------------------------------------------
 * cmonary_collection_find --
 *
 *      Performs a find query on a MongoDB collection, selecting certain fields
 *      from the results and storing them in Python lists.
 *
 *  Python Parameters:
 *      @db: A C string holding the name of the database.
 *      @coll: A C string holding the name of the collection.
 *      @offset: The number of documents to skip, or zero.
 *      @limit: The maximum number of documents to return, or zero.
 *      @query: A Python dictionary representing the query.
 *      @select_fields: [optional] If true, select exactly the fields from the
 *      database that match the specified fields.
 *
 *  Returns:
 *      A list of lists, each containing the desired data.
 *
 *  Side effects:
 *      None.
 *-------------------------------------------------------------------------
 */
static PyObject *
cmonary_collection_find (cmonary_t *self,
                         PyObject  *args,
                         PyObject  *kwargs)
{
    PyObject        *query_dict;
    bool             select_fields;
    bson_t           query_bson;
    bson_t          *fields_bson;
    const char      *coll;
    const char      *db;
    const uint8_t   *query;
    int32_t          query_size;
    mongoc_cursor_t *mcursor;
    static char     *kwarg_list;
    uint32_t         limit;
    uint32_t         offset;

    // Parse arguments
    kwarg_list = {
        "select_fields",
        NULL
    }
    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "ssIIO!|b",
                                     kwarg_list,
                                     &db,
                                     &coll,
                                     &offset,
                                     &limit,
                                     &PyDict_Type,
                                     &query_dict,
                                     &select_fields)) {
        return -1;
    }

    // Sanity checks
    if (!db) {
        PyErr_SetString(PyExc_ValueError, "db name cannot be empty");
        return -1;
    }
    else if (!coll) {
        PyErr_SetString(PyExc_ValueError, "collection name cannot be empty");
        return -1;
    }

    // Convert dictionary into raw bytes
    // TODO Steal PyMongo's BSON code

    // Build BSON query data
}

/*
 *-------------------------------------------------------------------------
 * cmonary_init --
 *
 *      Initializes the module.
 *-------------------------------------------------------------------------
 */
PyMODINIT_FUNC
cmonary_init (void) 
{
    PyObject* m;

    monary_type_t.tp_new = PyType_GenericNew;
    if (PyType_Ready(&monary_type_t) < 0)
        return;

    m = Py_InitModule3("monary", monary_methods,
                       "Performs blazingly-fast reads from MongoDB.");

    Py_INCREF(&monary_type_t);
    PyModule_AddObject(m, "Monary", (PyObject *) &monary_type_t);
}
