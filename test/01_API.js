'use strict';

const Multi_Data_Store = require( '../index.js' );
const tape = require( 'tape' );

tape( 'API: exports properly', t => {
    t.ok( Multi_Data_Store, 'module exports' );
    t.equal( Multi_Data_Store && typeof Multi_Data_Store.add_driver, 'function', 'exports add_driver method' );
    t.equal( Multi_Data_Store && typeof Multi_Data_Store.remove_driver, 'function', 'exports remove_driver method' );
    t.equal( Multi_Data_Store && typeof Multi_Data_Store.put, 'function', 'exports put method' );
    t.equal( Multi_Data_Store && typeof Multi_Data_Store.get, 'function', 'exports get method' );
    t.equal( Multi_Data_Store && typeof Multi_Data_Store.delete, 'function', 'exports delete method' );
    t.end();
} );
