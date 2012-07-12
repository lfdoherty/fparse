"use strict";

function readBoolean(buffer, offset){

	var result = buffer[offset] === 1;
	return result;
}

function readInt(buffer, offset){

	var result = 0;

	result += buffer[0+offset] << 24;
	result += buffer[1+offset] << 16;
	result += buffer[2+offset] << 8;
	result += buffer[3+offset] << 0;

	return result;
}

function readLong(buffer, offset){

	var first = readInt(buffer, offset);
	var second = readInt(buffer, offset+4);
	
	//require('sys').puts('r ' + first + ',' + second);
	
	var result = first + (second*2147483648);
	
	return result;
}

function writeBoolean(buffer, offset, value){
	buffer[offset] = value ? 1 : 0;
}

function writeInt(buffer, offset, value){

	buffer[0+offset] = value >> 24;
	buffer[1+offset] = value >> 16;
	buffer[2+offset] = value >> 8;
	buffer[3+offset] = value >> 0;
}	

function readUnsignedIntCompactly(buffer, offset){
	
	var b0 = buffer[0+offset];
	if(!(b0 & 128)) return [1,b0];
	var b1 = buffer[1+offset];
	if(!(b1 & 128)){
		return [2,(b0 & 127) + (b1 << 7)];
	}
	var b2 = buffer[2+offset];
	if(!(b2 & 128)){
		return [3,(b0 & 127) + ((b1 & 127) << 7) + (b2 << 14)];
	}
	var b3 = buffer[3+offset];
	if(!(b3 & 128)){
		return [4, (b0 & 127) + ((b1 & 127) << 7) + ((b2 & 127) << 14) + (b3 << 21)];
	}
	var b4 = buffer[4+offset];
	return [5, (b0 & 127) + ((b1 & 127) << 7) + ((b2 & 127) << 14) + ((b3 & 127) << 21) + (b4 << 28)];
}

function writeUnsignedIntCompactly(buffer, offset, value){
	if(value < 0) throw 'not unsigned';
	if(value < 128){
		buffer[0+offset] = value;
		return 1;
	}else if(value < 16384){
		buffer[0+offset] = value | 128; // 128 = 010000000 in binary - i.e. we set the high bit
		buffer[1+offset] = value >> 7;
		return 2;
	}else if(value < 2097152){
		buffer[0+offset] = value | 128;
		buffer[1+offset] = (value >> 7) | 128;
		buffer[2+offset] = value >> 14;
		return 3;
	}else if(value < 268435456){
		buffer[0+offset] = value | 128;
		buffer[1+offset] = (value >> 7) | 128;
		buffer[2+offset] = (value >> 14) | 128;
		buffer[3+offset] = value >> 21;
		return 4;
	}else{
		buffer[0+offset] = value | 128;
		buffer[1+offset] = (value >> 7) | 128;
		buffer[2+offset] = (value >> 14) | 128;
		buffer[3+offset] = (value >> 21) | 128;
		buffer[4+offset] = (value >> 28) & 15;
		return 5;
	}	
}
/*
function testCompactInts(){
	var m = 1024*1024;
	var k=0;
	for(var i=k*m;i<2048*m;i+=m){
		var b = new Buffer(5*m);
		var off = 0;
		for(var j=0;j<m;++j){
			var n = j+i;
			off += writeUnsignedIntCompactly(b, off, n);
		}
		off = 0;
		for(var j=0;j<m;++j){
			var n = j+i;
			var res = readUnsignedIntCompactly(b, off);
			if(n !== res[1]){
				for(var i=0;i<5;++i){
					console.log(b[off+i]);
				}
				console.log('error(' + res[0] + '), read(' + res[1] + ') does not match(' + n + ')');
				throw 'error';
			}
			off += res[0];
		}
		console.log('passed ' + k + '/2048 (' + n + ') (' + off + '/' + (5*m) + ' : ' + (off/(5*m)) + ')');
		++k;
	}
	console.log('compact int test passed');
	_.errout('done');
}
testCompactInts();*/

function writeLong(buffer, offset, value){

	var first = value, second = value >= 0 ? Math.floor(value / 2147483648) : Math.ceil(value / 2147483648);

	var sign = first > 0 ? 1 : -1;
	first = (sign*first) % 2147483648;
	
	first = sign*first;
	
	//require('sys').puts(value + ' k ' + (first + (second*2147483648)));
		
	//require('sys').puts('w ' + first + ',' + second);
	
	writeInt(buffer, offset, first);
	writeInt(buffer, offset+4, second);
}	

exports.readBoolean = readBoolean;
exports.writeBoolean = writeBoolean;

exports.readInt = readInt;
exports.writeInt = writeInt;

exports.readLong = readLong;
exports.writeLong = writeLong;

var sys = require('sys');

exports.readData = function(size, rs, cb, errorCb, endCb){

	var bigBuffer = new Buffer(size);
	var bigOffset = 0;
	var bigEndOffset = 0;

	rs.on('data', function(data){
	
		//sys.debug('got data: ' + data.length);
	
		if(data.length > bigBuffer.length - bigEndOffset){
			//sys.debug('making new buffer');
			if(bigEndOffset === bigOffset){
				//there is no buffered data, just replace
				var nb = new Buffer(Math.max(bigBuffer.length, data.length));
				bigOffset = 0;
				bigEndOffset = 0;				
				//sys.debug('buffer was empty');

				bigBuffer = nb;
			}else{
				//expand if necessary, and copy buffered data back to beginning
				var nb = new Buffer(Math.max(bigBuffer.length, (bigEndOffset-bigOffset)+data.length));
				
				bigBuffer.copy(nb, 0, bigOffset, bigEndOffset);
				bigEndOffset = (bigEndOffset - bigOffset);
				bigOffset = 0;
				
				bigBuffer = nb;
				//sys.debug('buffer was not empty');
			}
		}
			
		data.copy(bigBuffer, bigEndOffset, 0);
		bigEndOffset += data.length;
	
		var used = cb(bigBuffer, bigOffset, (bigEndOffset-bigOffset));
		bigOffset += used;
		
		/*
		if(data.length > bigBuffer.length - bigOffset){
			var temp = new Buffer(data.length + bigOffset);
			bigBuffer.copy(temp, 0, 0, bigOffset);
			bigBuffer = temp;
		}	
		
		data.copy(bigBuffer, bigOffset, 0);
		bigOffset += data.length;
		
		try{
			var used = cb(bigBuffer, 0, bigOffset);
		
			bigBuffer = bigBuffer.slice(used);
			bigOffset -= used; 
		}catch(e){
			if(errorCb) errorCb(e);
			throw e;//we cannot recover from this at the moment
		}	*/	
	});

	if(endCb){
		rs.on('end', endCb);
	}
};

var _ = require('underscorem'),
	fs = require('fs'),
	path = require('path');
	
function mkdirs(dir, cb){
	_.assertString(dir);
	if(dir.indexOf('/') === -1){
		path.exists(dir, function(exists){
			if(exists){
				cb();
			}else{
				fs.mkdir(dir, '0755', function(err){
					if(err) throw err;
					
					cb();
				});
			}
		});
	}else{
		var remDir = dir.substr(0, dir.lastIndexOf('/'));
		
		path.exists(dir, function(exists){
			if(exists){
				cb();
			}else{
				mkdirs(remDir, function(){				
					fs.mkdir(dir, '0755', function(err){
						if(err){
							if(err.code !== 'EEXIST'){
								throw err;
							}
						}
						
						cb();
					});
				});
			}
		});
	}
}
exports.mkdirs = mkdirs;

