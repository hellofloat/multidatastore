'use strict';

const async = require( 'async' );
const extend = require( 'extend' );

module.exports = {
    init: function( _options ) {
        this.options = extend( {
            id_field: 'id',
            async: true,
            async_callback: error => {
                if ( error ) {
                    console.error( error );
                }
            }
        }, _options );

        if ( !this.options.pool ) {
            throw new Error( 'Must specify a pg pool (https://github.com/brianc/node-postgres) object!' );
        }

        if ( !this.options.table ) {
            throw new Error( 'Must specify a table!' );
        }

        if ( !this.options.mapper ) {
            throw new Error( 'Must specify a mapper!' );
        }

        if ( !this.options.unmapper ) {
            throw new Error( 'Must specify an unmapper!' );
        }
    },

    put: function( object, callback ) {
        if ( this.options.async ) {
            callback();
            callback = this.options.async_callback;
        }

        const mapped_object = this.options.mapper( object );
        const mapped_object_data_keys = Object.keys( mapped_object ).sort().filter( key => key !== this.options.id_field );

        let client = null;
        let done = null;
        let exists = false;
        let query = null;

        async.series( [
            next => {
                this.options.pool.connect( ( error, _client, _done ) => {
                    client = _client;
                    done = _done;
                    next( error );
                } );
            },

            next => {
                client.query( {
                    text: `select 1 from ${ this.options.table } where ${ this.options.id_field }=($1)`,
                    values: [ mapped_object[ this.options.id_field ] ]
                }, ( error, result ) => {
                    exists = result && result.rows && result.rows.length;
                    next( error );
                } );
            },

            next => {

                let text = null;
                let values = null;

                if ( exists ) {
                    text = `
                        update ${ this.options.table }
                        set ${ mapped_object_data_keys.map( ( key, index ) => key + '=($' + ( index + 1 ) + ')' ).join( ', ' ) }
                        where ${ this.options.id_field }=($${ mapped_object_data_keys.length + 1 });`;

                    values = mapped_object_data_keys.map( key => mapped_object[ key ] );
                    values.push( mapped_object[ this.options.id_field ] );
                } else {
                    text = `
                        insert into ${ this.options.table } (
                            ${ [ this.options.id_field ].concat( mapped_object_data_keys ).join( ',' ) }
                        )
                        values (
                            ${ [ this.options.id_field ].concat( mapped_object_data_keys ).map( ( key, index ) => '$' + ( index + 1 ) ) }
                        );
                    `;

                    values = [ this.options.id_field ].concat( mapped_object_data_keys ).map( key => mapped_object[ key ] );
                }

                query = {
                    text,
                    values
                };

                next();
            },

            next => {
                client.query( query, next );
            }
        ], error => {
            if ( done ) {
                done( error );
            }

            client = null;

            callback( error );
        } );
    },

    get: function( id, callback ) {

        let client = null;
        let done = null;
        let db_read_result = null;
        let result = null;

        async.series( [
            next => {
                this.options.pool.connect( ( error, _client, _done ) => {
                    client = _client;
                    done = _done;
                    next( error );
                } );
            },

            next => {
                client.query( {
                    text: `select * from ${ this.options.table } where ${ this.options.id_field }=($1)`,
                    values: [ id ]
                }, ( error, _db_read_result ) => {
                    db_read_result = _db_read_result && _db_read_result.rows && _db_read_result.rows.length && _db_read_result.rows[ 0 ];
                    next( error );
                } );
            },

            next => {
                result = db_read_result ? this.options.unmapper( db_read_result ) : null;
                next();
            }
        ], error => {
            if ( done ) {
                done( error );
            }

            client = null;

            callback( error, result );
        } );
    },

    delete: function( id, callback ) {
        if ( this.options.async ) {
            callback();
            callback = this.options.async_callback;
        }

        let client = null;
        let done = null;

        async.series( [
            next => {
                this.options.pool.connect( ( error, _client, _done ) => {
                    client = _client;
                    done = _done;
                    next( error );
                } );
            },

            next => {
                client.query( {
                    text: `delete from ${ this.options.table } where ${ this.options.id_field }=($1)`,
                    values: [ id ]
                }, next );
            }
        ], error => {
            if ( done ) {
                done( error );
            }

            client = null;

            callback( error );
        } );
    }
};