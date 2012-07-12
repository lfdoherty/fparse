var _ = require('underscorem');
var bin = require('./bin');

var ccc = console.log

function makeReadState(){

	var cur,off
	
	var s = {
		readInt: function(){
			var v = bin.readInt(cur, off);
			off+=4;
			return v;
		},
		readString: function(len){
			_.assertInt(len)
			var v = cur.toString('utf8', off, off+len);
			off += len;
			return v;
		},
		readData: function(len){
			_.assertInt(len)
			var v = cur.slice(off, off+len)
			off += len;
			return v;
		},
		readVarData: function(){
			var len = s.readLength()
			var v = cur.slice(off, off+len)
			off += len;
			return v;
		},
		readVarString: function(){
			var len = s.readLength()
			return s.readString(len)
		},
		readReal: function(){
			var len = s.readLength()
			return Number(s.readString(len))
		},
		readByte: function(){
			var v = cur[off];
			++off
			return v;
		},
		readLong: function(){
			var v = bin.readLong(cur, off);
			off+=8;
			return v;
		},
		readBoolean: function(){
			var b = cur[off];

			++off;
			if(b !== 0 && b !== 1) console.log('b: ' + b)
			_.assert(b === 0 || b === 1);
			return b === 1;
		},
		readLength: function(){
			var count = s.readByte();
			if(count === 255){
				count += s.readInt();
			}
			return count;
		}
	}
	s.readVarUint = s.readLength
	
	return {
		s: s,
		assertEmpty: function(){
			_.assertEqual(off, cur.length);
		},
		put: function(buf){
			cur = buf;
			off = 0;
		}
	}
}
exports.make = makeReadState;