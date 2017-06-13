'use strict';

const extend = require( 'extend' );

module.exports = {
    init: function( _options ) {
        this.options = extend( {
            async: true,
            async_callback: error => {
                if ( error ) {
                    console.error( error );
                }
            }
        }, _options );

        if ( !this.options.connection ) {
            throw new Error( 'Must specify a connection object!' );
        }

        if ( !this.options.table ) {
            throw new Error( 'Must pass in a table object!' );
        }
    },

    put: function( object, callback ) {
        if ( this.options.async ) {
            callback();
            callback = this.options.async_callback;
        }

        this.options.table.insert( object, {
            conflict: 'replace'
        } ).run( this.options.connection, callback );
    },

    get: function( id, callback ) {
        this.options.table.get( id ).run( this.options.connection, callback );
    },

    delete: function( id, callback ) {
        if ( this.options.async ) {
            callback();
            callback = this.options.async_callback;
        }

        this.options.table.get( id ).delete().run( this.options.connection, callback );
    }
};