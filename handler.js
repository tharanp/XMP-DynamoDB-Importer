
'use strict';
var fs = require("fs");
var path = require("path");
var AWS = require('aws-sdk');
var async = require('async');
var s3 = new AWS.S3();
const S3_IMAGE_CSV_BUCKET = 'images-csv';
var tmpdir = '/tmp/';
var csv = require("fast-csv");
var csvfilename = 'Images.csv';
var outputcsvpath = tmpdir + csvfilename;
var docClient = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });  //change to your region
//Function to generate the metdata from dynamodb into single CSV File
exports.CsvCompiler = (event, context, callback) => {
    var update = 0;
    event.Records.forEach(function (record) {
        if (record.eventName == "MODIFY") {
            update = 1;
            console.log("found modified record");
        }
    }, this);
    if(event.Records.length>50)
    {
         update=1;
    }  
    if (update==1) {
        console.log("csv full data export  from db");
        csvFullDataExportFromDB(event, function (error, callback) {
            if (error) {
                console.log("failure condition task failed");
            }
            else {
                console.log("CompileCSV fnction success");
                context.done();
            }
        });
    }
    else {
        CompileCSV(event, function (error, callback) {
            if (error) {
               /*if csv import failed below function will execute and
               * get full data from db and write into csv*/
                csvFullDataExportFromDB(event, function (error, callback) {
                    if (error) {
                        console.log("failure condition task failed");                    }
                    else {
                        console.log("CompileCSV fnction success");
                        context.done();
                    }
                });

            }
            else {
                console.log("CompileCSV fnction success");
                context.done();
            }
        });
    }
};
/* Export full data from dynamodb and write into csv*/
var csvFullDataExportFromDB = function (event, callback) {
    var dataArray = [];
    async.waterfall([function (wcallback) {
        var params = {
            TableName: "digitalcollection",
            ProjectionExpression: "ObjectID,OriginalObjectNumber,FileName,ImageName,PixelH,PixelW,PrimaryDisplay,#status",
            ExpressionAttributeNames:{
                "#status":"STATUS"
            }
        };
        docClient.scan(params, function (err, data) {
            if (err) {
                console.log(err);
                wcallback(err);
            }
            else {
                console.log(data);
                data.Items.forEach(function(newImage) {                  
                    dataArray.push( { ObjectID: newImage.ObjectID, OriginalObjectNumber: newImage.OriginalObjectNumber, FileName: newImage.ImageName, PixelH: newImage.PixelH, PixelW: newImage.PixelW, PrimaryDisplay: newImage.PrimaryDisplay, STATUS: newImage.STATUS });
                   
                }, this);               
                wcallback(null);            
            }
        });      
    },
    function (wcallback) {       
        var csvWiteStream = csv.createWriteStream({ headers: true,quoteColumns: true,quoteHeaders: true,delimiter: "\t", escape:'"' }),
            writableStream = fs.createWriteStream(tmpdir + csvfilename);
        csvWiteStream.on("finish", function (error) {
            if (error) {
                wcallback(error);
            }
            else {
                wcallback(null);
            }
        });
        csvWiteStream.pipe(writableStream);
        dataArray.forEach(function (element) {          
            csvWiteStream.write(element);
        }, this);
        csvWiteStream.end();
    }, function (wcallback) {
        setTimeout(function () {
            var exists = fs.existsSync(tmpdir + csvfilename);          
            if (exists) {
                fs.readFile((tmpdir + csvfilename), function (err, data) {
                    if (err) {
                        console.log("error==" + err);
                        wcallback(err);
                    }
                    else {
                        var param = {
                            Bucket: S3_IMAGE_CSV_BUCKET,
                            Key: csvfilename,
                            Body: data
                        };
                        s3.putObject(param, function (err, data) {
                            if (err) {
                                console.log("error==" + err);
                                wcallback(err);
                            }
                            else {                              
                                wcallback(null);
                            }
                        });

                    }
                });               
            }
            else {              
                wcallback(new Error("file desnot exist"));
            }

        }, 5000);
    }], function (error, result) {
        if (error) {
            callback(error);
        }
        else {
            callback(null, true);
        }
    });
}
var CompileCSV = function (event, callback) {
    var i = 0;
    var obj = '';
    var dataArray = [];
    var removeAccNo = [];
    var removeObjID = [];
    async.waterfall([function (wcallback) {
        event.Records.forEach(function (record) {
            if (record.eventName == "INSERT") {
                var newImage = record.dynamodb.NewImage;
                var newEntry = { ObjectID: newImage.ObjectID.S, OriginalObjectNumber: newImage.OriginalObjectNumber.S, FileName:newImage.ImageName.S, PixelH: newImage.PixelH.N, PixelW: newImage.PixelW.N, PrimaryDisplay: newImage.PrimaryDisplay.S, STATUS: newImage.STATUS.S };
                dataArray.push(newEntry);
                console.log("New record tracked");
            }
            else if (record.eventName == "REMOVE") {
                var oldImage = record.dynamodb.OldImage;
                removeAccNo.push(oldImage.ImageName.S);  
                console.log("Removel tracked");              
            }                       
        }, this);
        wcallback(null);
    }, function (wcallback) {
        var params = {
            Bucket: S3_IMAGE_CSV_BUCKET,
            Key: csvfilename
        };
        const s3ReadStream = s3.getObject(params).
            on('error', function () { console.log(':( in error handler'); wcallback(null); }).createReadStream();
        var csvReadStream = csv({ headers: true,quoteColumns: true,quoteHeaders: true,delimiter: "\t", escape:'"' })
            .on('data', (data) => {
                if (data != undefined) {
                    console.log("csv data from s3 ", data);
                    dataArray.push(data);
                }
            }).on("end", () => {
                wcallback(null);
            }).on("error", function (error) {
                console.log("i am error 1", error);
                wcallback(error);
            });
        s3ReadStream.pipe(csvReadStream);
    },
    function (wcallback) {
        var csvWiteStream = csv.createWriteStream({ headers: true,quoteColumns: true,quoteHeaders: true,delimiter: "\t", escape:'"' }),
            writableStream = fs.createWriteStream(tmpdir + csvfilename);
        csvWiteStream.on("finish", function (error) {
            if (error) {
                wcallback(error);
            }
            else {
                wcallback(null);
            }
        });
        csvWiteStream.pipe(writableStream);
        console.log("data array before write", dataArray)
        dataArray.forEach(function (element) {
            if ((element.FileName != undefined) && ( removeAccNo.indexOf(element.FileName)==-1)) {
                csvWiteStream.write(element);
            }
        }, this);
        csvWiteStream.end();
    }, function (wcallback) {
        setTimeout(function () {
            var exists = fs.existsSync(tmpdir + csvfilename);           
            if (exists) {
                fs.readFile((tmpdir + csvfilename), function (err, data) {
                    if (err) {
                        wcallback(err);
                    }
                    else {                    
                        var param = {
                            Bucket: S3_IMAGE_CSV_BUCKET,
                            Key: csvfilename,
                            Body: data
                        };
                        s3.putObject(param, function (err, data) {
                            if (err) {
                                console.log("error==" + err);
                                wcallback(err);
                            }
                            else {                                
                                wcallback(null);
                            }
                        });

                    }
                });              
            }
            else {
                console.log("file desnot exist");
                wcallback(new Error("file desnot exist"));
            }
        }, 5000);
    }], function (error, result) {
        if (error) {
            callback(error);
        }
        else {
            callback(null, true);
        }
    });
}