var should= require('chai').should(),
    assert= require('chai').assert,
    AWS = require('aws-sdk'),
    dyngo=  require('../index.js');


describe('database',function ()
{
       var db;

       beforeEach(function (done)
       {
            dyngo({ dynamo: { endpoint: new AWS.Endpoint('http://localhost:8000') } },
            function (err,_db)
            {
               db= _db; 
               done(err);
            });
       });

       it('Can connect', function ()
       {
          should.exist(db);
       });
});
