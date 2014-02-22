var should= require('chai').should(),
    assert= require('chai').assert,
    AWS = require('aws-sdk'),
    dyngo=  require('../index.js');

const noerr= function (done)
      {
         return function (err)
         {
             should.not.exist(err);
             done();
         };
      };


describe('database',function ()
{
       var db;

       beforeEach(function (done)
       {
            dyngo({ dynamo: { endpoint: new AWS.Endpoint('http://localhost:8000') }, hints: false },
            function (err,_db)
            {
               db= _db; 
               db.test.remove().success(done)
                               .error(done); 
            });
       });

       it('Can connect', function (done)
       {
          should.exist(db);
          done();
       });

       describe('save',function ()
       {
           it('Can insert a new object, and then find it', function (done)
           {
                var _noerr= noerr(done);

                db.test.save({ somedata: 'ok' })
                       .success(function ()
                       {
                             db.test.findOne({ somedata: 'ok' })
                                    .result(function (obj)
                                    {
                                          obj.somedata.should.equal('ok'); 
                                          done();
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object, and have it back', function (done)
           {
                var _noerr= noerr(done);

                db.test.save({ somedata: 'parent', child: { somedata: 'child' } })
                       .success(function ()
                       {
                             db.test.findOne({ somedata: 'parent' })
                                    .result(function (obj)
                                    {
                                          obj.somedata.should.equal('parent'); 
                                          obj.child.somedata.should.equal('child'); 
                                          done();
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object, and then remove the child', function (done)
           {
                var _noerr= noerr(done);

                var par= { somedata: 'parentrchild', child: { somedata: 'child' } };

                db.test.save(par)
                       .success(function ()
                       {
                             delete par.child;

                             db.test.save(par)
                                    .success(function ()
                                    {
                                         db.test.findOne({ $id: par.$id })
                                                .result(function (obj)
                                                {
                                                      obj.somedata.should.equal('parentrchild'); 
                                                      should.not.exist(obj.child); 
                                                      done();
                                                })
                                                .error(_noerr);
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });
       });

});
