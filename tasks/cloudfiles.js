module.exports = function(grunt) {
  var pkgcloud = require('pkgcloud'),
      path = require('path'),
      util = require('util'),
      async = require('async'),
      crypto = require('crypto'),
      fs = require('fs'),
      _ = grunt.util._;

  var client;

  grunt.registerMultiTask('cloudfiles', 'Move stuff to the cloud', function() {
    var done = this.async(),
        config = this.data,
        enableCdn = false;

    var clientConfig = _.omit(config, ['enableCdn', 'upload', 'user', 'pass', 'key']);

    if (!config.hasOwnProperty('provider')) {
      clientConfig.provider = 'rackspace';
    }
    if (clientConfig.provider == 'rackspace') {
      enableCdn = config.enableCdn !== false;
    }

    if (config.hasOwnProperty('user')){
      clientConfig.username = config.user;
    }
    if (config.hasOwnProperty('pass')){
      clientConfig.password = config.pass;
    }
    if (config.hasOwnProperty('key')){
      clientConfig.apiKey = config.key;
    }

    client = pkgcloud.storage.createClient(clientConfig);

    async.forEach(config.upload, function(upload, next) {
      grunt.log.subhead('Uploading into ' + upload.container);

      client.getContainer(upload.container, function(err, container) {
        // client error
        if (err && !(err.statusCode === 404)) {
          return next(err);
        }
        // 404, so create it
        else if (err && err.statusCode === 404) {
          grunt.log.write('Creating CDN Enabled Container: ' + upload.container);
          createContainer(upload.container, enableCdn, function(err, container) {
            if (err) {
              return next(err);
            }

            syncFiles(upload, container, next);
          });
        }
        // created, but not cdn enabled
        else if (container && !container.cdnEnabled && enableCdn) {
          grunt.log.write('CDN Enabling Container: ' + upload.container);
          container.enableCdn(function(err, container) {
            if (err) {
              return next(err);
            }

            syncFiles(upload, container, next);
          });
        }
        // good to go, just sync the files
        else {
          syncFiles(upload, container, function (err) {
            if (!err && upload.hasOwnProperty("purge")) {
                purgeFiles(upload, container, next);
            } else {
                next(err);
            }
          });
        }
      });
    }, function(err) {
      if (err) {
        grunt.log.error(err);
      }
      done(err);
    });
  });

  function purgeFiles(upload, container, callback) {
      grunt.log.subhead('Purging files from ' + upload.container);
      async.forEachLimit(upload.purge.files, 10, function (fileName, next) {
          grunt.log.writeln('Purging ' + fileName);
          client.purgeFileFromCdn(container, fileName, upload.purge.emails || [], function (err) {
              if (err) {
                  grunt.log.error(err);
              }
              next();
          })
      }, callback);
  }

  function syncFiles(upload, container, callback) {
    grunt.log.writeln('Syncing files to container: ' + container.name);

    var files = grunt.file.expand(upload.src);

    if (upload.dest === undefined) { upload.dest = '' }

    var maxAttemptsPerFile = 5;

    async.forEachLimit(files, 10, function(file, next) {

      var attempt = 1;

      var onSyncFile = function(err) {
        if(err) {
            grunt.log.writeln('Error syncing file: ' + file + ' to ' + container.name + ', attempt: ' + attempt + '/' + maxAttemptsPerFile);

            attempt++;

            if(attempt < maxAttemptsPerFile) {
                syncFile(file, container, upload.dest, upload.stripcomponents, upload.headers, onSyncFile);
            } else {
                next(err);
            }

        } else {
            next();
        }
      }

      if (grunt.file.isFile(file)) {
        syncFile(file, container, upload.dest, upload.stripcomponents, upload.headers, onSyncFile);
      }
      else {
        next();
      }
    }, function(err) {
      callback(err);
    });
  }

  function syncFile(fileName, container, dest, strip, headers, callback) {

    var ufile = fileName;
    headers = headers || {};

    if (strip !== undefined) {
      ufile = stripComponents(ufile, strip);
    }

    if (dest) {
        ufile = dest + ufile;
    }

    hashFile(fileName, function (err, hash) {
      if (err) {
        return next(err);
      }

      client.getFile(container, ufile, function (err, file) {
        if (err && !(err.statusCode === 404)) {
          callback(err);
        }
        else if (err && err.statusCode === 404) {
          grunt.log.writeln('Uploading ' + fileName + ' to ' + container.name + ' (NEW)');
          client.upload({
            container: container,
            remote: ufile,
            local: fileName,
            headers: headers
          }, function (err) {
            callback(err);
          });
        }
        else if (file && file.etag !== hash) {
          grunt.log.writeln('Updating ' + fileName + ' to ' + container.name + ' (MD5 Diff)');
          client.upload({
            container: container,
            remote: ufile,
            local: fileName,
            headers: headers
          }, function (err) {
            callback(err);
          });
        }
        else {
          grunt.log.writeln('Skipping ' + fileName + ' in ' + container.name + ' (MD5 Match)');
          callback();
        }
      })
    });
  }

  function createContainer(containerName, enableCdn, callback) {
    client.createContainer(containerName, function(err, container) {
      if (err) {
        return callback(err);
      }

      if (enableCdn) {
        container.enableCdn(function(err, container) {
          if (err) {
            return callback(err);
          }

          callback(err, container);
        });
      }
      else {
        callback(err, container);
      }
    });
  }

  function stripComponents(path, num, sep) {
    if (sep === undefined) sep = '/';
    var aString = path.split(sep)
    if (aString.length <= num) {
      return aString[aString.length - 1];
    } else {
      aString.splice(0, num);
      return aString.join(sep);
    }
  }

  // Used to MD5 a file, useful when checking against already
  // uploaded assets
  function hashFile(filename, callback) {

    var calledBack = false,
        md5sum = crypto.createHash('md5'),
        stream = fs.ReadStream(filename);

    stream.on('data', function (data) {
      md5sum.update(data);
    });

    stream.on('end', function () {
      var hash = md5sum.digest('hex');
      callback(null, hash);
    });

    stream.on('error', function(err) {
      handleResponse(err);
    });

    function handleResponse(err, hash) {
      if (calledBack) {
        return;
      }

      calledBack = true;
      callback(err, hash);
    }
  }
}
