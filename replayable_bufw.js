"use strict";

var _ = require('underscorem');
var sys = require('util'),
	Buffer = require('buffer').Buffer,
	bin = require('./bin'),
	fs = require('fs');


function W(bufferSize, ws){
	_.assertLength(arguments, 2);
	_.assertInt(bufferSize)
	//_.assertFunction(ws.end)
	_.assertObject(ws);

	this.ws = ws;

	this.position = 0;
	
	this.bufferSize = bufferSize;
	//console.log(new Error().stack)

	this.replayableBuffers = []//where buffers that have been flushed go until they have been discarded from replayability
	this.replayableOffset = 0//so that we don't have to slice buffers

	this.b = new Buffer(bufferSize);
	this.bytesSinceFlush = 0;
	
	this.countStack = [];
	this.countValueStack = [];
	this.countPos = -1;
	this.lengthStack = [];
	this.lenPos = -1;
	
	var local = this;
	
	/*setInterval(function(){
		var reps = []
		local.replayableBuffers.forEach(function(rb){
			reps.push(rb.length)
		})
		//console.log(JSON.stringify({b: local.b.length, replayables: reps, replayableOffset: local.replayableOffset},null,2))
	},10000)*/
}


W.prototype.writeBuffer = function(nextSize, cb){	

	if(this.countPos >= 0){
		_.errout('cannot write buffer in the middle of counting: ' + this.countPos);
	}
	if(this.lenPos >= 0){
		_.errout('cannot write buffer in the middle of length: ' + this.lenPos);
	}

	if(this.delayed){
		_.errout('cannot write buffer while delayed');
	}
	
	if(this.position === 0) return

	this.bytesSinceFlush += this.position;

	nextSize = 1;
	nextSize = Math.min(this.bufferSize, Math.max(nextSize, this.bytesSinceFlush))+1024;
	//console.log('(' + this.position + ')nextSize: ' + this.bufferSize + ' ' + nextSize + ' ' + this.bytesSinceFlush)
	//console.log(new Error().stack)

	var local = this;
	var bb = this.b;

	var writingBuffer = this.b.length > this.position ? this.b.slice(0, this.position) : this.b;

	this.b = new Buffer(nextSize);

	this.position = 0;
	
	this.needWrite = false;

	//console.log('writing buffer: ' + writingBuffer.length)
	//console.log(new Error().stack)
	
	this.replayableBuffers.push(writingBuffer)
	
	var res = this.ws.write(writingBuffer);
	
	if(!res && cb){
		this.ws.once('drain', cb);
	}else if(cb){
		cb();
	}
}

W.prototype.discardReplayable = function(manyBytes){
	_.assert(this.replayableBuffers.length > 0)
	while(manyBytes > 0 && manyBytes >= this.replayableBuffers[0].length - this.replayableOffset){
		_.assert(this.replayableBuffers.length > 0)
		manyBytes -= this.replayableBuffers[0].length - this.replayableOffset
		this.replayableOffset = 0
		//console.log('discarding buffer: ' + this.replayableBuffers[0].length + ' ' + manyBytes)
		this.replayableBuffers.shift()
	}
	
	if(manyBytes === 0) return
	
	_.assert(this.replayableBuffers.length > 0)
	//console.log('discarding partial: ' + manyBytes)
	var remaining = 0
	var local = this
	this.replayableBuffers.forEach(function(b,index){
		if(index === 0){
			remaining += b.length - local.replayableOffset
		}else{
			remaining += b.length
		}
	})
	/*console.log('remaining: ' + remaining)
	var reps = []
	local.replayableBuffers.forEach(function(rb){
		reps.push(rb.length)
	})
	console.log('buffers: ' + JSON.stringify(reps))*/
	this.replayableOffset += manyBytes
}
W.prototype.replay = function(){
	
	var local = this
	this.replayableBuffers.forEach(function(b,index){
		if(index === 0 && local.replayableOffset > 0){
			b = b.slice(local.replayableOffset)
		}
		//console.log('replaying buffer: ' + b.length)
		local.ws.write(b)
	})
}

W.prototype.prepareFor = function(manyBytes){
	if(this.b.length - this.position < manyBytes){

		if(this.delayed || this.lenPos >= 0 || this.countPos >= 0){
			//console.log('need to write: ' + this.b.length + ' ' + this.position + ' ' + manyBytes)
			//console.log(new Error().stack)
			//console.log('expanding delayed: ' + this.position)
			this.needWrite = true;
			var nb = new Buffer((manyBytes+this.b.length)*2);
			this.b.copy(nb, 0, 0);
			this.b = nb;
		}else{
			this.writeBuffer(manyBytes*2);
			if(this.b.length < manyBytes){
				this.b = new Buffer(manyBytes*2);
			}
		}	
	}
}
W.prototype.putString = function(str){
	var len = Buffer.byteLength(str);
	if(len >= 255){
		this.prepareFor(len+5);
		this.putByte(255);
		this.putInt(len-255);
	}else{
		this.prepareFor(len+1);
		this.putByte(len);
	}
	this.b.write(str, this.position, 'utf8');
	this.position += len;
}
W.prototype.putVarUint = function(i){
	if(i >= 255){
		this.prepareFor(5);
		this.putByte(255);
		this.putInt(i-255);
	}else{
		this.prepareFor(1);
		this.putByte(i);
	}
}
W.prototype.putVarData = function(buf, off, len){
	off = off || 0
	len = len !== undefined ? len : (buf.length-off);
	//console.log('len: ' + len)
	if(len >= 255){
		this.prepareFor(len+5);
		this.putByte(255);
		this.putInt(len-255);
	}else{
		this.prepareFor(len+1);
		this.putByte(len);
	}
	buf.copy(this.b, this.position, off, len)
	this.position += len;
}

W.prototype.putBuffer = function(buf, len){
	if(len === undefined) len = buf.length;
	this.prepareFor(len+4);
	this.putInt(len);
	buf.copy(this.b, this.position, 0, len);
	this.position += len;
}
W.prototype.putBufferDirectly = function(buf, off, len){

	_.assertInt(off);
	_.assertInt(len);

	this.prepareFor(len+4);
	this.putInt(len);
	buf.copy(this.b, this.position, off, off+len);
	this.position += len;
}
W.prototype.putData = function(buf, off, len){
	/*if(length === undefined) length = buf.length;
	this.prepareFor(length);
	buf.copy(this.b, this.position, 0, length);
	this.position += length;*/
	off = off || 0
	len = len !== undefined ? len : (buf.length-off);
	this.prepareFor(len);
	//this.putInt(len);
	buf.copy(this.b, this.position, off, len)
	this.position += len;
	//console.log('putData ' + new Error().stack)
}
W.prototype.putByte = function(v){
	this.prepareFor(1);
	this.b[this.position] = v;
	++this.position;
}
W.prototype.putBoolean = function(v){
	this.prepareFor(1);
	//bin.writeBoolean(this.b, this.position, v);
	this.b[this.position] = v ? 1 : 0;
	++this.position
}
W.prototype.putInt = function(v){
	this.prepareFor(4);
	bin.writeInt(this.b, this.position, v);
	this.position += 4;
}
W.prototype.putUuid = function(buf){
	_.assertBuffer(buf)
	if(buf.length !== 16) throw new Error('invalid uuid buffer: ' + buf + ', length is ' + buf.length)
	this.prepareFor(16);
	//bin.writeInt(this.b, this.position, v);
	buf.copy(this.b, this.position, 0, 16);
	this.position += 16;
}
W.prototype.putLong = function(v){
	this.prepareFor(8);
	bin.writeLong(this.b, this.position, v);
	this.position += 8;
}
W.prototype.putReal = function(v){
	this.putString(''+v)
}
W.prototype.flush = function(cb){
	var cc = this.position;
	this.writeBuffer(undefined, cb);
	this.bytesSinceFlush = 0;
	return cc;
}
W.prototype.flushAndDie = function(cb){
	var cc = this.position;
	//this.writeBuffer(undefined, cb);
	//this.bytesSinceFlush = 0;
	//return cc;
	//console.log('flushing ' + this.position)
	var writingBuffer = this.b.length > this.position ? this.b.slice(0, this.position) : this.b;
	this.position = undefined;
	this.needWrite = false;
	this.b = undefined
	this.ws.write(writingBuffer);
}

W.prototype.close = function(cb, skipWrite){
	if(!skipWrite){
		this.writeBuffer();
	}
	
	this.ws.end(cb);
}

W.prototype.startCount = function(){
	this.prepareFor(4);

	++this.countPos;
	this.countStack[this.countPos] = this.position;
	this.countValueStack[this.countPos] = 0;

	this.position += 4;
	this.delayed = true;
}
W.prototype.countUp = function(n){
	if(n === undefined){
		n = 1;
	}
	this.countValueStack[this.countValueStack.length-1] += n;
}
W.prototype.endCount = function(){

	var pos = this.countStack[this.countPos];
	var c = this.countValueStack[this.countPos];

	--this.countPos;
	bin.writeInt(this.b, pos, c);
}

W.prototype.startLength = function(){
	this.prepareFor(4);

	++this.lenPos;
	this.lengthStack[this.lenPos] = this.position;

	this.position += 4;
}
W.prototype.currentLength = function(){
	var writePos = this.lengthStack[this.lenPos];
	var len = (this.position - writePos) - 4;
	return len
}
W.prototype.endLength = function(){

	var writePos = this.lengthStack[this.lenPos];
	--this.lenPos;

	var len = (this.position - writePos) - 4;
	bin.writeInt(this.b, writePos, len);
	return len
}

W.prototype.delay = function(){
	if(this.delayed) _.errout('error, cannot delay multiple times');
	this.delayed = true;
	this.delayPoint = this.position;
}
W.prototype.resume = function(){
	this.delayed = false;
	if(this.lenPos === -1 && this.countPos === -1 && !this.delayed && this.needWrite){
		this.writeBuffer();
	}
}
W.prototype.cancel = function(){
	this.position = this.delayPoint;
	this.delayed = false;
}

W.prototype.getBackingBuffer = function(){
	return this.b
}
W.prototype.getCurrentOffset = function(){
	return this.position
}

exports.W = W;
