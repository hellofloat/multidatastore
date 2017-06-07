'use strict';

const async = require( 'async' );
const extend = require( 'extend' );
const Multi_Data_Store = require( '../index.js' );
const tape = require( 'tape' );

const alasql = require( 'alasql' );
const sqlstring = require( 'sqlstring' );
const Postgres_Driver = require( '../drivers/postgres' );
const Rethink_Driver = require( '../drivers/rethinkdb' );
const S3_Driver = require( '../drivers/s3' );

const Postgres_Mock_Pool = {
    connect: function( callback ) {
        this.client = this.client || {
            query: function( params, query_done ) {
                const query_string = params.text.replace( /\$\d+/g, '?' );
                const query_string_with_values = sqlstring.format( query_string, params.values );
                alasql
                    .promise( query_string_with_values )
                    .then( _result => {
                        const result = {
                            rows: Array.isArray( _result ) ? _result : [ _result ]
                        };

                        // console.log( query_string_with_values );
                        // console.dir( result );

                        query_done( null, result );
                    } )
                    .catch( query_done );
            }
        };

        callback( null, this.client, error => {
            if ( error ) {
                console.error( error );
            }
        } );
    }
};

const Rethink_Mock_Table = {
    insert: function( object ) {
        return {
            run: ( connection, callback ) => {
                connection[ object.id ] = object;
                callback();
            }
        };
    },

    get: function( id ) {
        return {
            run: ( connection, callback ) => {
                callback( null, connection[ id ] );
            },

            delete: () => {
                return {
                    run: ( connection, callback ) => {
                        delete connection[ id ];
                        callback();
                    }
                }
            }
        };
    }
};

const S3_Mock_Store = {
    upload: function( params, callback ) {
        this.store = this.store || {};
        this.store[ params.Bucket ] = this.store[ params.Bucket ] || {};
        this.store[ params.Bucket ][ params.Key ] = {
            ContentType: params.ContentType,
            Body: params.Body
        };
        callback();
    },

    getObject: function( params, callback ) {
        this.store = this.store || {};
        const bucket = this.store[ params.Bucket ] || {};
        const result = bucket[ params.Key ];
        callback( null, result );
    },

    deleteObject: function( params, callback ) {
        this.store = this.store || {};
        this.store[ params.Bucket ] = this.store[ params.Bucket ] || {};
        delete this.store[ params.Bucket ][ params.Key ];
        callback();
    }
};

function get_multiple_store_setup( _options ) {
    const options = extend( {
        readable: {}
    }, _options );

    const multi_data_store = Object.create( Multi_Data_Store );

    const rethink_driver = Object.create( Rethink_Driver );
    const rethink_table = Object.create( Rethink_Mock_Table );
    const rethink_connection = {};
    rethink_driver.init( {
        async: false,
        readable: options.readable.rethink || false,
        table: rethink_table,
        connection: rethink_connection
    } );
    multi_data_store.add_driver( rethink_driver );

    const postgres_driver = Object.create( Postgres_Driver );
    const postgres_mock_pool = Object.create( Postgres_Mock_Pool );
    alasql( 'CREATE TABLE IF NOT EXISTS test_multiple_stores (id string, data string)' );
    postgres_driver.init( {
        async: false,
        readable: options.readable.postgres || false,
        pool: postgres_mock_pool,
        table: 'test_multiple_stores',
        mapper: object => {
            return {
                id: object.id,
                data: object.test
            };
        },
        unmapper: result => {
            return {
                id: result.id,
                test: result.data
            };
        }
    } );
    multi_data_store.add_driver( postgres_driver );

    const s3_driver = Object.create( S3_Driver );
    s3_driver.init( {
        async: false,
        readable: options.readable.s3 || false,
        s3: Object.create( S3_Mock_Store ),
        bucket: 'test.multiple_stores.bucket',
        get_id_path: id => `test/${ id }.json`,
        get_object_path: object => `test/${ object.id }.json`
    } );
    multi_data_store.add_driver( s3_driver );

    return multi_data_store;
}

tape( 'multiple stores: put', t => {
    const multi_data_store = get_multiple_store_setup();

    const id = Math.floor( Math.random() * 10000 );

    async.series( [
        next => {
            multi_data_store.put( {
                id: id,
                test: 'one'
            }, next );
        }
    ], error => {
        t.error( error, 'no errors' );
        t.end();
    } );
} );

tape( 'multiple stores: put (update)', t => {
    const multi_data_store = get_multiple_store_setup( {
        readable: {
            rethink: true
        }
    } );

    const id = Math.floor( Math.random() * 10000 );

    async.series( [
        next => {
            multi_data_store.put( {
                id: id,
                test: 'foo'
            }, error => {
                t.error( error, 'put test object (initial)' );
                next( error );
            } );
        },

        next => {
            multi_data_store.put( {
                id: id,
                test: 'bar'
            }, error => {
                t.error( error, 'put test object (update)' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( id, ( error, result ) => {
                t.ok( result, 'got result from store' );
                t.equal( result && result.id, id, 'id is correct' );
                t.equal( result && result.test, 'bar', 'content is correct' );
                next( error );
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        t.end();
    } );
} );

tape( 'multiple stores: get (readable rethink)', t => {
    const multi_data_store = get_multiple_store_setup( {
        readable: {
            rethink: true
        }
    } );

    const id = Math.floor( Math.random() * 10000 );

    async.series( [
        next => {
            multi_data_store.put( {
                id: id,
                test: 'foo'
            }, error => {
                t.error( error, 'put test object' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( id, ( error, result ) => {
                t.ok( result, 'got result from store' );
                t.equal( result && result.id, id, 'id is correct' );
                t.equal( result && result.test, 'foo', 'content is correct' );
                next( error );
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        t.end();
    } );
} );

tape( 'multiple stores: get (readable postgres)', t => {
    const multi_data_store = get_multiple_store_setup( {
        readable: {
            postgres: true
        }
    } );

    const id = Math.floor( Math.random() * 10000 );

    async.series( [
        next => {
            multi_data_store.put( {
                id: id,
                test: 'foo'
            }, error => {
                t.error( error, 'put test object' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( id, ( error, result ) => {
                t.ok( result, 'got result from store' );
                t.equal( result && result.id, id, 'id is correct' );
                t.equal( result && result.test, 'foo', 'content is correct' );
                next( error );
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        t.end();
    } );
} );

tape( 'multiple stores: get (readable s3)', t => {
    const multi_data_store = get_multiple_store_setup( {
        readable: {
            s3: true
        }
    } );

    const id = Math.floor( Math.random() * 10000 );

    async.series( [
        next => {
            multi_data_store.put( {
                id: id,
                test: 'foo'
            }, error => {
                t.error( error, 'put test object' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( id, ( error, result ) => {
                t.ok( result, 'got result from store' );
                t.equal( result && result.id, id, 'id is correct' );
                t.equal( result && result.test, 'foo', 'content is correct' );
                next( error );
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        t.end();
    } );
} );

tape( 'multiple stores: get (unreadable)', t => {
    const multi_data_store = get_multiple_store_setup();

    const id = Math.floor( Math.random() * 10000 );

    async.series( [
        next => {
            multi_data_store.put( {
                id: id,
                test: 'foo'
            }, error => {
                t.error( error, 'put test object' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( id, error => {
                t.ok( error, 'got error' );
                t.equal( error && error.error, 'missing readable driver', 'got: missing readable driver' );
                next();
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        t.end();
    } );
} );