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
            },
            get_id_path: () => {
                return false;
            }
        }, _options );

        if ( !this.options.s3 ) {
            throw new Error( 'Must pass in an s3 object!' );
        }

        if ( !this.options.bucket ) {
            throw new Error( 'Must speicify an s3 bucket!' );
        }

        if ( !this.options.get_object_path ) {
            throw new Error( 'Must specify a get_object_path function!' );
        }
    },

    put: function( object, callback ) {
        const path = this.options.get_object_path( object );
        if ( !path ) {
            callback( {
                error: 'invalid object path'
            } );
            return;
        }

        if ( this.options.async ) {
            callback();
            callback = this.options.async_callback;
        }

        try {
            const data = JSON.stringify( object, null, 4 );

            this.options.s3.upload( {
                Bucket: this.options.bucket,
                ContentType: 'application/json',
                Key: path,
                Body: data
            }, callback );
        }
        catch ( ex ) {
            callback( new Error( 'Could not convert object to JSON format!' ) );
        }
    },

    get: function( id, callback ) {
        const path = this.options.get_id_path( id );
        if ( !path ) {
            callback( {
                error: 'invalid id path'
            } );
            return;
        }

        this.options.s3.getObject( {
            Bucket: this.options.bucket,
            Key: path
        }, ( error, response ) => {
            if ( error ) {
                callback( error );
                return;
            }

            let object = null;

            try {
                object = JSON.parse( response && response.Body );
            }
            catch( ex ) {
                object = null;
                error = ex;
            }

            callback( error, object );
        } );
    },

    delete: function( id, callback ) {
        const path = this.options.get_id_path( id );
        if ( !path ) {
            callback( {
                error: 'invalid id path'
            } );
            return;
        }

        if ( this.options.async ) {
            callback();
            callback = this.options.async_callback;
        }

        this.options.s3.deleteObject( {
            Bucket: this.options.bucket,
            Key: path
        }, callback );
    }
};