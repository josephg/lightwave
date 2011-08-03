if (!window.lightwave) {
    lightwave = {};
}

if (!lightwave.ot) {
    lightwave.ot = {};
}

// ------------------------------------------------------------------
// TombStream

lightwave.ot.TombStream = function(seq) {
    this.seq = seq;
    this.pos = 0;
    this.inside = 0;
};

lightwave.ot.TombStream.prototype.InsertChars = function(n) {
    if (n == 0) {
    // Do nothing by intention
    } else if (this.pos == this.seq.length) { // Insert at EOF (implying that the seq is empty)
        this.seq.push(n);
        this.inside = n;
    } else if (this.seq[this.pos] >= 0) { // Insert inside a character sequence
        this.seq[this.pos] = this.seq[this.pos] + n;
        this.inside += n;
    } else if (this.inside == -this.seq[this.pos]) { // End of a tomb sequence?
        this.pos++;
        this.inside = 0;
        this.InsertChars(n);
    } else if (this.inside == 0) { // Beginning of a tomb sequence?
        if (this.pos > 0) { // Go to the end of the previous character sequence
            this.pos--;
            this.inside = this.seq[this.pos];
            this.InsertChars(n);
        }
        this.seq.splice(this.pos, 0, n);
        this.inside = n;
    } else { // Insert inside a tomb sequence
        this.seq.splice(this.pos + 1, 0, n);
        this.seq.splice(this.pos + 2, 0, this.seq[this.pos] + this.inside);
        this.seq.splice(this.pos, 0, -this.inside);
        this.pos++;
        this.inside = n;
    }
};

lightwave.ot.TombStream.prototype.InsertTombs = function(n) {
    if (n == 0) {
        // Do nothing by intention
    } else if (this.pos == this.seq.length) { // Insert at EOF (implying that the seq is empty)
        this.seq.push(-n);
        this.inside = n;
    } else if (this.seq[this.pos] < 0) { // Insert inside a tomb sequence
        this.seq[this.pos] = this.seq[this.pos] - n;
        this.inside += n;
    } else if (this.inside == this.seq[this.pos]) { // End of a character sequence?
        this.pos++;
        this.inside = 0;
        this.InsertTombs(n);
    } else if (this.inside == 0) { // Beginning of a character sequence?
        if (this.pos > 0) { // Go to the end of the previous tomb sequence
            this.pos--;
            this.inside = -this.seq[this.pos];
            this.InsertTombs(n);
        }
        this.seq.splice(this.pos, 0, -n);
        this.inside = n;
    } else { // Insert inside a character sequence
        this.seq.splice(this.pos + 1, 0, -n );
        this.seq.splice(this.pos + 2, 0, this.seq[this.pos] - this.inside);
        this.seq[this.pos] = this.inside;
        this.pos++;
        this.inside = n;
    }
};

lightwave.ot.TombStream.prototype.Bury = function(n) {
    var burried = 0, err;
    while (n != 0) {
        if (this.pos == this.seq.length) { // End of the sequence -> error
            err = lightwave.NewError("Burry reached EOF");
            return [burried, err];
        }
        var x = this.seq[this.pos];
        if (x < 0) { // Bury characters that have already been burried?
            var m = Math.min(n, -x - this.inside);
            this.inside += m;
            n -= m;
            if (this.inside == -x && n > 0) {
	        this.pos++;
	        this.inside = 0;
            }
            continue;
        }
        var m = Math.min(n, x - this.inside);
        n -= m;
        burried += m;
        this.seq[this.pos] = -m;
        var left = this.inside;
        var right = x - m - this.inside;
        this.inside = m;
        if (left > 0) {
            this.seq.splice(this.pos, 0, left);
            this.pos++;
        } else if (this.pos > 0) {
            this.seq.splice(this.pos, 1);
            this.pos--;
            this.seq[this.pos] = this.seq[this.pos] - m;
            this.inside = -this.seq[this.pos];
        }
        if (right > 0) {
            this.seq.splice(this.pos + 1, 0, right)
        } else if (this.pos + 1 < this.seq.length) {
            this.seq[this.pos] = this.seq[this.pos] + this.seq[this.pos + 1];
            this.seq.splice(this.pos + 1, 1);
        }
    }
    return [burried, err];
};

lightwave.ot.TombStream.prototype.Skip = function(n) {
    var chars = 0, err;
    while (n > 0) {
        if (this.pos >= this.seq.length) {
            return [chars, lightwave.NewError("TombStream reached EOF")];
        }
        var x = this.seq[this.pos];
        if (x >= 0) {
            if (this.inside == x) {
	        this.pos++;
	        this.inside = 0;
	        continue;
            }
            var m = Math.min(x - this.inside, n);
            this.inside += m;
            n -= m;
            chars += m;
        } else {
            if (this.inside == -x) {
	        this.pos++;
	        this.inside = 0;
	        continue;
            }
            var m = Math.min(-x - this.inside, n);
            this.inside += m;
            n -= m;
        }
    }
    return [chars, null];
};

lightwave.ot.TombStream.prototype.SkipChars = function(n) {
    var skipped  = 0, err;
    while (n > 0) {
        if (this.pos >= this.seq.length) {
            return [skipped, lightwave.NewError("TombStream reached EOF")];
        }
        var x = this.seq[this.pos];
        var x2 = x
        if (x < 0) {
            x2 = -x;
        }
        if (this.inside == x2) {
            this.pos++;
            this.inside = 0;
            continue;
        }
        var m = Math.min(x2 - this.inside, n);
        this.inside += m;
        skipped += m;
        if (x >= 0) {
            n -= m;
        }
    }
    return [skipped, err];
};

lightwave.ot.TombStream.prototype.SkipToEnd = function() {
    var count = 0;
    while (this.pos < this.seq.length) {
        var x = this.seq[this.pos];
        if (x < 0) {
            x = -x;
        }
        count += x - this.inside;
        this.inside = 0;
        this.pos++;
    }
    return count;
};

// ------------------------------------------------------------------
// Execution of mutations

lightwave.ot.ExecuteOperation = function(input, op) {
    var err;
    if (!op) {
        return [input, null];
    }
    if (op["$t"]) {
        if (!input) {
            input = new lightwave.ot.SimpleText("");
        }
        err = lightwave.ot.executeString(input, op["$t"]);
        return [input, err];
    } else {
        err = lightwave.NewError("Apply: Operation not allowed in this place")
    }
    return [input, err];
};

// Apply a mutation to the input document
lightwave.ot.executeString = function(text, ops) {
    text.Begin();
    for ( var i = 0; i < ops.length; i++) {
        var op = ops[i];
        if (!op ) {
            continue;
        } else if (typeof(op) == "string") {
	    text.InsertChars(op);
        } else if ( op["$t"] ) {
	    text.InsertTombs(op["$t"]);
        } else if ( op["$s"] ) {
            var err = text.Skip(op["$s"]);
            if (err) {
                text.End();
                return err;
            }
        } else if ( op["$d"] ) {
            var err = text.Delete(op["$d"]);
            if (err) {
                text.End();
                return err;
            }
        }
    }
    text.End();
    return null;
};

// ----------------------------------------------------------
// SimpleText

lightwave.ot.SimpleText = function(text) {
    this.Text = text;
    this.tombs = [text.length];
};

lightwave.ot.SimpleText.prototype.Begin = function() {
    this.tombStream = new lightwave.ot.TombStream(this.tombs);
    this.pos = 0;
};

lightwave.ot.SimpleText.prototype.InsertChars = function(str) {
    this.tombStream.InsertChars(str.length);
    this.Text = this.Text.substring(0, this.pos) + str + this.Text.substring(this.pos, this.Text.length);
    this.pos += str.length;
};

lightwave.ot.SimpleText.prototype.InsertTombs = function(count) {
    this.tombStream.InsertTombs(count);
};

lightwave.ot.SimpleText.prototype.Delete = function(count) {
    var burried, err;
    var result = this.tombStream.Bury(count);
    burried = result[0];
    err = result[1];
    if (err) {
        return err;
    }
    this.Text = this.Text.substring(0, this.pos) + this.Text.substring(this.pos + burried, this.Text.length);
    return null;
};

lightwave.ot.SimpleText.prototype.Skip = function(count) {
    var chars = 0, err;
    var result = this.tombStream.Skip(count);
    chars = result[0];
    err = result[1];
    if (err) {
        return err;
    }
    this.pos += chars;
    return null;
};

lightwave.ot.SimpleText.prototype.End = function() {
    delete this.tombStream;
}
