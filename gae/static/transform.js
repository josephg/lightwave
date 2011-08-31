if (!window.lightwave) {
    lightwave = {};
}

if (!lightwave.ot) {
    lightwave.ot = {};
}

lightwave.NewError = function(msg) {
    return msg;
};

// -------------------------------------------------------------------------
// A stream of operations

lightwave.ot.Stream = function(ops) {
    this.ops = ops;
    // An index inside the ops slice
    this.pos = 0;
    // An index that points inside an operation.
    // For example it tells how many characters of an InsertOp have
    // already been read.
     this.inside = 0;
};

lightwave.ot.Stream.prototype.IsEOF = function() {
    return this.pos == this.ops.length;
}

lightwave.ot.cloneOp = function(op) {
    var result = {};
    for(var key in op) {
        result[key] = op[key];
    }
    return result;
};

lightwave.ot.opLen = function(op) {
    if (op["i"]) {
        return op["i"].length;
    }
    if (op["d"]) {
        return op["d"];
    }
    if (op["s"]) {
        return op["s"];
    }
    if (op["t"]) {
        return op["t"];
    }
    return 0;
};

// Extract an operation of the required length.
// Length is either -1 or limited by the remaining length of the current operation (i.e. this.ops[this.pos].Len - this.inside).
lightwave.ot.Stream.prototype.Read = function(len) {
    var op = lightwave.ot.cloneOp(this.ops[this.pos])
    var op_len = lightwave.ot.opLen(op);
    if (len == -1) {
        len = op_len - this.inside;
    }
    if (typeof(op["i"]) != "undefined") {
        if (this.inside == 0 && len == op_len) {
            // Do nothing by intention
        } else {
            if (op_len > 0) {
	        op = op.substring(this.inside, this.inside + len)
            } else {
	        op = null;
            }
        }
    }
    this.inside += len
    if (this.inside == op_len) {
        this.inside = 0;
        this.pos++;
    }
    if (op["d"]) {
        op["d"] = len;
    } else if (op["s"]) {
        op["s"] = len;
    } else if (op["t"]) {
        op["t"] = len;
    }
    return op;
};

// -------------------------------------------------------------------------
// Read pairs of operations from two streams

// The reader is used during transformation.
// It reads a pair of operations, from two operation streams.
// If one operation is InsertOp or if the other stream is already finished, then
// the other operation can be NoOp.
// Transformation is implemented by reading a pair operations, transforming the ops, and reading again ...
lightwave.ot.Reader = function(stream1, stream2) {
    this.stream1 = stream1;
    this.stream2 = stream2;
};

// Read a tuple of operations from stream1 and stream2
lightwave.ot.Reader.prototype.Read = function() {
    var op1, op2
    // EOF?
    if (this.stream1.IsEOF() && this.stream2.IsEOF()) {
        return [op1, op2, null];
    }
    // EOF Stream1?
    if (this.stream1.IsEOF()) {
        if (this.stream2.ops[this.stream2.pos]["i"] || this.stream2.ops[this.stream2.pos]["t"]) {
            op2 = this.stream2.Read(-1);
            return [op1, op2, null];
        }
        return [null, null, lightwave.NewError("Streams have different len")];
    }
    // EOF Stream2?
    if (this.stream2.IsEOF()) {
        if (this.stream1.ops[this.stream1.pos]["i"] || this.stream1.ops[this.stream1.pos]["t"]) {
            op1 = this.stream1.Read(-1);
            return [op1, op2, null];
        }            
        return [null, null, lightwave.NewError("Streams have different len")];
    }
    // Insert of stream1 goes first
    if (this.stream1.ops[this.stream1.pos]["i"] || this.stream2.ops[this.stream2.pos]["t"]) {
        op1 = this.stream1.Read(-1);
        return [op1, op2, null];
    }
    // Insert of stream2 goes next
    if (this.stream2.ops[this.stream2.pos]["i"] || this.stream1.ops[this.stream1.pos]["t"]) {
        op2 = this.stream2.Read(-1);
        return [op1, op2, null]
    }
    op1_len = lightwave.ot.opLen(this.stream1.ops[this.stream1.pos]);
    op2_len = lightwave.ot.opLen(this.stream2.ops[this.stream2.pos]);
    // Skip, and Delete go together
    var l = Math.min(op1_len - this.stream1.inside, op2_len - this.stream2.inside)
    op1 = this.stream1.Read( l ) 
    op2 = this.stream2.Read( l )
    return [op1, op2, null]
};

// -------------------------------------------------------------------------
// Transformation of mutations

// Transform a sequence of operations
lightwave.ot.transformStringOperations = function(ops1, ops2, f) {
    var tops1 = [], tops2 = [], err;
    var reader = new lightwave.ot.Reader(new lightwave.ot.Stream(ops1), new lightwave.ot.Stream(ops2));
    while (true) {
        var result = reader.Read();
        var op1 = result[0];
        var op2 = result[1];
        err = result[2];
        // Error or EOF?
        if (err || (!op1 && !op2)) {
            return [tops1, tops2, err];
        }
        result = ligthwave.ot.transformStringOperation(op1, op2)
        op1 = result[0];
        op2 = result[1];
        err = result[2];
        if (err) {
            return [tops1, tops2, err];
        }
        if (op1) {
            tops1.push(op1);
        }
        if (op2) {
            tops2.push(op2);
        }
    }
    return [tops1, tops2, err];
};

// Transform a pair of operations that works on a string
lightwave.ot.transformStringOperation = function(op1, op2) {
    var top1, top2, err;
    if (op1 && !op1["i"] && !op1["d"] && !op1["s"] && !op1["t"]) {
        err = lightwave.NewError("Operation not allowed in a string");
        return [top1, top2, err];
    }
    if (op2 && !op2["i"] && !op2["d"] && !op2["s"] && !op2["t"]) {
        err = lightwave.NewError("Operation not allowed in a string");
        return [top1, top2, err];
    }
    top1 = op1;
    top2 = op2;
    if (op1 && (op1["i"] || op1["t"])) {
        top2 = {"s": lightwave.ot.opLen(op1)};
    } else if (op2 && (op2["i"] || op2["t"])) {
        top1 = {"s": lightwave.ot.opLen(op2)};
    }
    return [top1, top2, err];
};