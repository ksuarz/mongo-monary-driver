#include <Python/Python.h>
#include <mongoc.h>
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

    if (self->coldata) {
        length = PyList_Size(self->coldata);
        for (i = 0; i < length; i++) {
            sublist = (PyListObject *) PyList_GetItem(self->coldata, i);
            if (sublist) {
                Py_DECREF(sublist);
            }
            else {
                break;
            }
        }
        Py_DECREF(self->coldata);
    }
    Py_XDECREF(self->client);
    Py_XDECREF(self->collection);
    self->ob_type->tp_free((PyObject *) self);
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
cmonary_new (PyTypeObject *type, /* IN */
             PyObject     *args) /* IN */
{
    cmonary_t *self;
    const char *uri;
    const char *db;
    const char *collection;

    if (!PyArg_ParseTuple(args, "sss", uri, db, collection)) {
        return NULL;
    }

    self = (cmonary_t *) type->tp_alloc(type, 0);
    if (self) {
        self->coldata = PyList_New(0);
        if (!self->coldata) {
            Py_DECREF(self);
            return NULL;
        }
        self->client = NULL;
        self->collection = NULL;
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
cmonary_init (cmonary_t *self,      /* IN */
              PyObject  *args,      /* IN */
              PyObject  *kwargs)    /* IN */
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

static PyObject *
cmonary_easy_query (cmonary_t *self)
{
    uint8_t             *query;
    mongoc_cursor_t     *cursor;
    mongoc_collection_t *collection;
    bson_t               query_bson;

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
 *      @lists: a pointer to a list of Python lists.
 *      @cursor: a MongoDB cursor from libmongoc.
 *
 *  Returns:
 *      The total number of failed loads.
 *-------------------------------------------------------------------------
 */
int
_cmonary_load_cursor (PyListObject      ***lists,  /* IN-OUT */
                      const PyListObject  *fields, /* IN */
                      mongoc_cursor_t     *cursor) /* IN */
{
    bson-iter_t iter;
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
_cmonary_load_cursor_single (PyListObject   ***lists,  /* IN-OUT */
                             const char       *field,  /* IN */
                             mongoc_cursor_t  *cursor) /* IN */
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
int
_cmonary_load_item (bson_iter_t    *iter,
                    cmonary_list_t *list)
{
    int32_t   int32_val;
    int64_t   int64_val;
    PyObject *item;

    switch (bson_iter_type(iter)) {
        case BSON_TYPE_DOUBLE:
            return 0;
        case BSON_TYPE_UTF8:
            return 0;
        case BSON_TYPE_DOCUMENT:
            return 0;
        case BSON_TYPE_ARRAY:
            return 0;
        case BSON_TYPE_BINARY:
            return 0;
        case BSON_TYPE_OID:
            return 0;
        case BSON_TYPE_BOOL:
            return 0;
        case BSON_TYPE_DATE_TIME:
            return 0;
        case BSON_TYPE_INT32:
            int32_val = bson_iter_int32(iter);
            item = PyInt_FromLong(int32_val);
            break;
        case BSON_TYPE_INT64:
            int64_val = bson_iter_int64(iter);
            item = PyInt_FromLong(int64_val);
            break;
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
 *      TODO see the monary.py interface
 *
 *  Returns:
 *      A list of lists, each containing the desired data.
 *
 *  Side effects:
 *      None.
 *-------------------------------------------------------------------------
 */
static PyObject *
cmonary_query (cmonary_t *self,
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
    // TODO

    // Build BSON query data
}

static PyTypeObject monary_MonaryType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "monary.Monary",             /*tp_name*/
    sizeof(monary_MonaryObject), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    0,                         /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,        /*tp_flags*/
    "A Monary object.",           /* tp_doc */
};

static PyMethodDef monary_methods[] = {
    {NULL}
};

void
monary_init (void) 
{
    PyObject* m;

    monary_MonaryType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&monary_MonaryType) < 0)
        return;

    m = Py_InitModule3("monary", monary_methods,
                       "Monary provides high-performance queries from MongoDB.");

    Py_INCREF(&monary_MonaryType);
    PyModule_AddObject(m, "Monary", (PyObject *) &monary_MonaryType);
}

static PyObject *
monary_connect (PyObject   *self,
                const char *uri)
{
    mongoc_client_t* client;
    if (!uri) {
        Py_RETURN_NONE;
    }
    
    
}

static PyObject *
monary_query (PyObject *self, /* IN */
              PyObject *args) /* IN */
{
    bool select_fields;
    bson_t *fields_bson;
    bson_t query_bson;
    const uint8_t *query;
    int32_t query_size;
    mongoc_cursor_t *cursor;
    uint32_t limit;
    uint32_t offset;

    if (!self->collection || !query) {
        Py_RETURN_NONE;
    }
}
