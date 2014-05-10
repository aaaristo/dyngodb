var should= require('chai').should(),
    assert= require('chai').assert,
    AWS = require('aws-sdk'),
    _= require('underscore'),
    dyngo=  require('../index.js');

const noerr= function (done)
      {
         return function (err)
         {
             should.not.exist(err);
             done();
         };
      },
      accept= function (code,done)
      {
         return function (err)
         {
           if (err.code==code)
             done(); 
           else
             done(err);
         };
      };

describe('transactions',function ()
{
       var db;

       before(function (done)
       {
            dyngo({ dynamo: { endpoint: new AWS.Endpoint('http://localhost:8000') }, hints: false },
            function (err,_db)
            {
               db= _db; 

               db.ensureTransactionTable().success(done).error(done);
            });
       });

       beforeEach(function (done)
       {
            db.test.remove().success(done)
                            .error(done); 
       });

       it('If transaction A inserts a new object transaction B cannot see it while not committed (transient)',
       function (done)
       {
          db.transaction().transaction(function (A)
          {
             db.transaction().transaction(function (B)
             {
                 A.test.save({ _id: 'transient' }).success(function ()
                 {
                    B.test.findOne({ _id: 'transient', _pos: 0 })
                          .result(should.not.exist)
                          .error(accept('notfound',done));
                 }).error(done);
             }).error(done);
          }).error(done);
       });

       it('If transaction A inserts a new object, while there was a transient object already present, transaction B cannot see it while not committed (transient)',
       function (done)
       {
          db.transaction().transaction(function (A)
          {
             db.transaction().transaction(function (B)
             {
                 A.test.save({ _id: 'transient2' }).success(function ()
                 {
                     A.test.save({ _id: 'transient2' }).success(function ()
                     {
                        B.test.findOne({ _id: 'transient2', _pos: 0 })
                              .result(should.not.exist)
                              .error(accept('notfound',done));
                     }).error(done);
                 }).error(done);
             }).error(done);
          }).error(done);
       });

       it('If transaction A inserts a new object, and commit, transaction B can see the new object',
       function (done)
       {
          db.transaction().transaction(function (A)
          {
             db.transaction().transaction(function (B)
             {
                 A.test.save({ _id: 'transient3' }).success(function ()
                 {
                     A.commit().committed(function ()
                     {
                        B.test.findOne({ _id: 'transient3', _pos: 0 })
                              .result(function (r)
                              {
                                  should.exist(r);
                                  r._id.should.equal('transient3');
                                  done();
                              })
                              .error(done);
                     }).error(done);
                 }).error(done);
             }).error(done);
          }).error(done);
       });

       it('If transaction A deletes an object and does not commit, transaction B can see the object, but transaction A can\'t',
       function (done)
       {
          db.test.save({ _id: 'delete1' }).success(function ()
          {
              db.transaction().transaction(function (A)
              {
                 db.transaction().transaction(function (B)
                 {
                     A.test.remove({ _id: 'delete1' }).success(function ()
                     {
                         B.test.findOne({ _id: 'delete1', _pos: 0 })
                               .result(function (r)
                               {
                                  should.exist(r);
                                  r._id.should.equal('delete1');

                                  A.test.findOne({ _id: 'delete1', _pos: 0 })
                                        .result(should.not.exists)
                                        .error(accept('notfound',done));
                               })
                               .error(done);

                     }).error(done);
                 }).error(done);
              }).error(done);
          }).error(done);
       });

       it('If transaction A deletes an object and commit, transaction B cannot see the object',
       function (done)
       {
          db.test.save({ _id: 'delete2' }).success(function ()
          {
              db.transaction().transaction(function (A)
              {
                 db.transaction().transaction(function (B)
                 {
                     A.test.remove({ _id: 'delete2' }).success(function ()
                     {
                         A.commit().committed(function ()
                         {
                             B.test.findOne({ _id: 'delete2', _pos: 0 })
                                   .result(should.not.exist)
                                   .error(accept('notfound',done));
                         }).error(done);
                     }).error(done);
                 }).error(done);
              }).error(done);
          }).error(done);
       });
});
