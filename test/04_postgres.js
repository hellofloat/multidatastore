'use strict';

const alasql = require( 'alasql' );
const async = require( 'async' );
const Multi_Data_Store = require( '../index' );
const sqlstring = require( 'sqlstring' );
const Postgres_Driver = require( '../drivers/postgres' );

const tape = require( 'tape' );

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

tape( 'postgres: put', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const postgres_driver = Object.create( Postgres_Driver );
    const postgres_mock_pool = Object.create( Postgres_Mock_Pool );

    alasql( 'CREATE TABLE test (id string, data string)' );

    postgres_driver.init( {
        async: false,
        pool: postgres_mock_pool,
        table: 'test',
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

    multi_data_store.put( {
        id: '1',
        test: 'one'
    }, error => {
        t.error( error, 'able to put' );
        alasql( 'DROP TABLE test' );
        t.end();
    } );
} );

tape( 'postgres: put (update)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const postgres_driver = Object.create( Postgres_Driver );
    const postgres_mock_pool = Object.create( Postgres_Mock_Pool );

    alasql( 'CREATE TABLE test (id string, data string)' );

    postgres_driver.init( {
        async: false,
        readable: true,
        pool: postgres_mock_pool,
        table: 'test',
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

    async.series( [
        next => {
            multi_data_store.put( {
                id: '1',
                test: 'foo'
            }, error => {
                t.error( error, 'put test object (initial)' );
                next( error );
            } );
        },

        next => {
            multi_data_store.put( {
                id: '1',
                test: 'bar'
            }, error => {
                t.error( error, 'put test object (update)' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( '1', ( error, result ) => {
                t.ok( result, 'got result from store' );
                t.equal( result && result.id, '1', 'id is correct' );
                t.equal( result && result.test, 'bar', 'content is correct' );
                next( error );
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        alasql( 'DROP TABLE test' );
        t.end();
    } );
} );

tape( 'postgres: get (readable)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const postgres_driver = Object.create( Postgres_Driver );
    const postgres_mock_pool = Object.create( Postgres_Mock_Pool );

    alasql( 'CREATE TABLE test (id string, data string)' );

    postgres_driver.init( {
        async: false,
        readable: true,
        pool: postgres_mock_pool,
        table: 'test',
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

    async.series( [
        next => {
            multi_data_store.put( {
                id: '1',
                test: 'foo'
            }, error => {
                t.error( error, 'put test object' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( '1', ( error, result ) => {
                t.ok( result, 'got result from store' );
                t.equal( result && result.id, '1', 'id is correct' );
                t.equal( result && result.test, 'foo', 'content is correct' );
                next( error );
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        alasql( 'DROP TABLE test' );
        t.end();
    } );
} );

tape( 'postgres: get (unreadable)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const postgres_driver = Object.create( Postgres_Driver );
    const postgres_mock_pool = Object.create( Postgres_Mock_Pool );

    alasql( 'CREATE TABLE test (id string, data string)' );

    postgres_driver.init( {
        async: false,
        pool: postgres_mock_pool,
        table: 'test',
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

    async.series( [
        next => {
            multi_data_store.put( {
                id: '1',
                test: 'foo'
            }, error => {
                t.error( error, 'put test object' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( '1', error => {
                t.ok( error, 'got error' );
                t.equal( error && error.error, 'missing readable driver', 'got: missing readable driver' );
                next();
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        alasql( 'DROP TABLE test' );
        t.end();
    } );
} );