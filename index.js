'use strict';

const async = require( 'async' );
const extend = require( 'extend' );

module.exports = {
    add_driver: function( driver ) {
        this.drivers = this.drivers || [];
        this.drivers.push( driver );
        return true;
    },

    remove_driver: function( driver ) {
        this.drivers = this.drivers || [];
        const driver_index = this.drivers.indexOf( driver );

        if ( driver_index < 0 ) {
            return false;
        }

        this.drivers.splice( driver_index, 1 );
        return true;
    },

    put: function( object, callback ) {
        this.drivers = this.drivers || [];
        async.each( this.drivers, ( driver, next ) => {
            driver.put( object, next );
        }, callback );
    },

    get: function( id, callback ) {
        this.drivers = this.drivers || [];
        const readable_driver = this.drivers.find( driver => {
            return driver && driver.options && driver.options.readable;
        } );

        if ( !readable_driver ) {
            callback( {
                error: 'missing readable driver'
            } );
            return;
        }

        readable_driver.get( id, callback );
    },

    delete: function( id, callback ) {
        this.drivers = this.drivers || [];
        async.each( this.drivers, ( driver, next ) => {
            driver.delete( id, next );
        }, callback );
    }
};