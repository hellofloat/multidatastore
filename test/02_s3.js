'use strict';

const async = require( 'async' );
const Multi_Data_Store = require( '../index' );
const S3_Driver = require( '../drivers/s3' );

const tape = require( 'tape' );

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

        if ( !result ) {
            callback( {
                code: 'NoSuchKey',
                message: 'he resource you requested does not exist'
            } );
            return;
        }

        callback( null, result );
    },

    deleteObject: function( params, callback ) {
        this.store = this.store || {};
        this.store[ params.Bucket ] = this.store[ params.Bucket ] || {};
        const exists = !!this.store[ params.Bucket ][ params.Key ];

        if ( !exists ) {
            callback( {
                code: 'NoSuchKey',
                message: 'he resource you requested does not exist'
            } );
            return;
        }

        delete this.store[ params.Bucket ][ params.Key ];
        callback();
    }
};

tape( 's3: put', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const s3_driver = Object.create( S3_Driver );

    s3_driver.init( {
        async: false,
        s3: Object.create( S3_Mock_Store ),
        bucket: 'test.bucket',
        get_object_path: object => {
            return `test/${ object.id }.json`;
        }
    } );

    multi_data_store.add_driver( s3_driver );

    multi_data_store.put( {
        id: '1',
        test: 'one'
    }, error => {
        t.error( error, 'able to put' );
        t.end();
    } );
} );

tape( 's3: get (readable)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const s3_driver = Object.create( S3_Driver );

    s3_driver.init( {
        async: false,
        readable: true,
        s3: Object.create( S3_Mock_Store ),
        bucket: 'test.bucket',
        get_id_path: id => {
            return `test/${ id }.json`;
        },
        get_object_path: object => {
            return `test/${ object.id }.json`;
        }
    } );

    multi_data_store.add_driver( s3_driver );

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

tape( 's3: get (unreadable)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const s3_driver = Object.create( S3_Driver );

    s3_driver.init( {
        async: false,
        s3: Object.create( S3_Mock_Store ),
        bucket: 'test.bucket',
        get_id_path: id => {
            return `test/${ id }.json`;
        },
        get_object_path: object => {
            return `test/${ object.id }.json`;
        }
    } );

    multi_data_store.add_driver( s3_driver );

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

tape( 's3: get (invalid id path)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const s3_driver = Object.create( S3_Driver );

    s3_driver.init( {
        async: false,
        readable: true,
        s3: Object.create( S3_Mock_Store ),
        bucket: 'test.bucket',
        get_object_path: object => {
            return `test/${ object.id }.json`;
        }
    } );

    multi_data_store.add_driver( s3_driver );

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
                t.equal( error && error.error, 'invalid id path', 'got: invalid id path' );
                next();
            } );
        }
    ], error => {
        t.error( error, 'no errors' );
        t.end();
    } );
} );

tape( 's3: delete', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const s3_driver = Object.create( S3_Driver );

    s3_driver.init( {
        async: false,
        readable: true,
        s3: Object.create( S3_Mock_Store ),
        bucket: 'test.bucket',
        get_id_path: id => {
            return `test/${ id }.json`;
        },
        get_object_path: object => {
            return `test/${ object.id }.json`;
        }
    } );

    multi_data_store.add_driver( s3_driver );

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
        },

        next => {
            multi_data_store.delete( '1', error => {
                t.error( error, 'deleted object' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( '1', error => {
                t.ok( error, 'got error' );
                t.equal( error && error.code, 'NoSuchKey', 'got: NoSuchKey' );
                next();
            } );
        }

    ], error => {
        t.error( error, 'no errors' );
        t.end();
    } );
} );

tape( 's3: delete (ignore_delete)', t => {
    const multi_data_store = Object.create( Multi_Data_Store );
    const s3_driver = Object.create( S3_Driver );

    s3_driver.init( {
        async: false,
        readable: true,
        ignore_delete: true,
        s3: Object.create( S3_Mock_Store ),
        bucket: 'test.bucket',
        get_id_path: id => {
            return `test/${ id }.json`;
        },
        get_object_path: object => {
            return `test/${ object.id }.json`;
        }
    } );

    multi_data_store.add_driver( s3_driver );

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
        },

        next => {
            multi_data_store.delete( '1', error => {
                t.error( error, 'deleted object' );
                next( error );
            } );
        },

        next => {
            multi_data_store.get( '1', ( error, result ) => {
                t.ok( result, 'got result from store, even after delete' );
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