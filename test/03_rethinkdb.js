'use strict';

const async = require( 'async' );
const Multi_Data_Store = require( '../index' );
const Rethink_Driver = require( '../drivers/rethinkdb' );

const tape = require( 'tape' );

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

tape( 'rethink: put', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const rethink_driver = Object.create( Rethink_Driver );
    const rethink_table = Object.create( Rethink_Mock_Table );
    const rethink_connection = {};

    rethink_driver.init( {
        async: false,
        table: rethink_table,
        connection: rethink_connection
    } );

    multi_data_store.add_driver( rethink_driver );

    multi_data_store.put( {
        id: '1',
        test: 'one'
    }, error => {
        t.error( error, 'able to put' );
        t.end();
    } );
} );

tape( 'rethink: get (readable)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const rethink_driver = Object.create( Rethink_Driver );
    const rethink_table = Object.create( Rethink_Mock_Table );
    const rethink_connection = {};

    rethink_driver.init( {
        async: false,
        readable: true,
        table: rethink_table,
        connection: rethink_connection
    } );

    multi_data_store.add_driver( rethink_driver );

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
        t.end();
    } );
} );

tape( 'rethink: get (unreadable)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const rethink_driver = Object.create( Rethink_Driver );
    const rethink_table = Object.create( Rethink_Mock_Table );
    const rethink_connection = {};

    rethink_driver.init( {
        async: false,
        table: rethink_table,
        connection: rethink_connection
    } );

    multi_data_store.add_driver( rethink_driver );

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
        t.end();
    } );
} );
