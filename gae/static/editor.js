
if ( !window.LW ) {
    LW = { };
}

/**
  * A collaborative editor for rich text objects.
  *
  * @constructor
  * @param text is an instance of LW.Richtext
  */
LW.Editor = function(entity, dom) {
    this.entity = entity;
    // Register event handler at the entity
    this.entity.listener = this;

    /**
     * The HTML div element that is being edited.
     */
    this.dom = dom;
    this.blockViewUpdates = false;

    // Install event handlers
    var self = this;
    dom.onkeypress = function(e) { self.keypress(e); }
    dom.onkeydown = function(e) { self.keydown(e); }
    dom.onkeyup = function(e) { self.keyup(e); }
    
    // Render the document
    this.dom.innerHTML = "";
    for( var i = 0; i < this.entity.paragraphs.length; ++i ) {
        this.renderParagraph(i, true);
    }
};

/**
 * Handle non-printable keys, i.e. backspace or delete.
 */
LW.Editor.prototype.keydown = function(e) {
    window.console.log("KeyDown code=" + e.keyCode.toString());

    // Only backspace and delete are handled here. Everything else passes.
    if ( e.keyCode != 8 && e.keyCode != 46 )
        return;
        
    var sel = window.getSelection();
        
    // Delete selection?
    if ( !sel.isCollapsed && (e.keyCode == 8 || e.keyCode == 46)) {
        window.console.log("DeleteSelection");
        e.stopPropagation();
        e.preventDefault();
        this.deleteSelection(true);
        return;
    }

    var selDom = sel.anchorNode;
    var selOffset = sel.anchorOffset;
    var pos = this.getTextPosition( selDom, selOffset );

    // Backspace?
    if ( e.keyCode == 8 ) {
        // Create Doc op
        var mut = [];
        var count = this.otCharCountTo_(pos) - 1;
        if ( count > 0 ) {
            mut.push({"$s": count});
        }
        mut.push({"$d": 1});
        count = this.otCharCountFrom_(pos);
        if ( count > 0 ) {
            mut.push({"$s": count});
        }

        // Delete a linebreak?
        if ( pos.charCount == 0 ) {
            e.stopPropagation();
            e.preventDefault();

            // At the beginning of the document? -> Do nothing
            if ( pos.paragIndex == 0 )
                return;

            // Position the cursor
            var parag = this.entity.paragraphs[pos.paragIndex - 1];
            pos = { paragIndex: pos.paragIndex - 1, charCount: parag.text.length };

            // Execute doc op locally in the DOM
            this.submitMutation(mut);

            var domPos = this.getDomPosition(pos);
            sel.collapse( domPos.element, domPos.index );
            return;
        }

        // Chrome inserts some tags when a line becomes empty. Don't want this.
        // Thus, I do the deletion myself and end up with a nice <div><br><div>
        var parag = this.entity.paragraphs[pos.paragIndex];
        if ( parag.text.length == 1 ) {
            e.stopPropagation();
            e.preventDefault();                
            pos = {paragIndex: pos.paragIndex, charCount: 0};

            // Submit doc op and execute it locally
            this.submitMutation(mut);

            var domPos = this.getDomPosition(pos);
            sel.collapse( domPos.element, domPos.index );
            return
        }

        // Submit doc op but do NOT execute it in the DOM. Let the editor do it
        this.blockViewUpdates = true;
        this.submitMutation(mut);
        this.blockViewUpdates = false;
    }
    // Delete?
    else if ( e.keyCode == 46 ) {
        // Create Doc op
        var mut = [];
        var count = this.otCharCountTo_(pos);
        if ( count > 0 ) {
            mut.push({"$s": count});
        }
        mut.push({"$d": 1});
        count = this.otCharCountFrom_(pos) - 1;
        if ( count > 0 ) {
            mut.push({"$s": count});
        }

        var parag = this.entity.paragrapgs[pos.paragIndex];
        // Delete a line break?
        if ( parag.text.length == pos.charCount ) {
            e.stopPropagation();
            e.preventDefault();

            // End of document? -> Do nothing
            if ( pos.paragIndex + 1 == this.entity.paragraphs.length )
                return;
            
            // Submit doc op and execute it locally
            this.submitMutation(mut);

            var domPos = this.getDomPosition(pos);
            sel.collapse( domPos.element, domPos.index );
            return;
        }
        
        // Chrome inserts some tags when a line becomes empty. Don't want this.
        // Thus, I do the deletion myself and end up with a nice <div><br><div>
        var parag = this.entity.paragrapgs[pos.paragIndex];
        if ( parag.text.length == 1 ) {
            e.stopPropagation();
            e.preventDefault();
            
            pos = {paragIndex: pos.paragIndex, charCount: 0};
 
            // Submit doc op and execute it locally
            this.submitMutation(mut);

            // Position the cursor behind the character we just inserted
            var domPos = this.getDomPosition(pos);
            sel.collapse( domPos.element, domPos.index );
            return
        }

        this.blockViewUpdates = true;
        this.submitMutation(mut);
        this.blockViewUpdates = false;
    }
    else
        throw "Unsupported keycode";
};

/**
 * Handle printable keys, i.e. letters, numbers, enter/return
 */
LW.Editor.prototype.keypress = function(e) {
    window.console.log("Key Press = " + e.keyIdentifier);

    var sel = window.getSelection();

    // Delete selection?
    if ( !sel.isCollapsed ) {
        this.deleteSelection(true);
    }

    var selDom = sel.anchorNode;
    var selOffset = sel.anchorOffset;
    var pos = this.getTextPosition( selDom, selOffset );
    var parag = this.entity.paragraphs[pos.paragIndex];

    if ( e.keyIdentifier == "Enter" || e.keyCode == 13) {
        window.console.log("Return");
        e.stopPropagation();
        e.preventDefault();

        // Create Doc op and execute it locally
        var mut = [];
        var count = this.otCharCountTo_(pos);
        if ( count > 0 ) {
            mut.push({"$s": count});
        }
        mut.push("\n");
        count = this.otCharCountFrom_(pos);
        if ( count > 0 ) {
            mut.push({"$s": count});
        }
        this.submitMutation(mut);

        pos = {paragIndex: pos.paragIndex + 1, charCount: 0};
        var domPos = this.getDomPosition(pos);
        sel.collapse( domPos.element, domPos.index );
        return;
    }

    // Create a doc op
    var mut = [];
    var count = this.otCharCountTo_(pos);
    if ( count > 0 ) {
        mut.push({"$s": count});
    }
    mut.push(String.fromCharCode(e.charCode));
    count = this.otCharCountFrom_(pos);
    if ( count > 0 ) {
        mut.push({"$s": count});
    }

    // First character? Needs some special treatment -> let the OT logic do it
    if ( pos.charCount == 0 && parag.text.length == 0 ) {
        e.stopPropagation();
        e.preventDefault();

        pos = {paragIndex: pos.paragIndex, charCount: 1};

        // Submit the doc op and let it execute locally in the DOM
        this.submitMutation(mut);

        var domPos = this.getDomPosition(pos);
        sel.collapse( domPos.element, domPos.index );
        return;
    }
     
    // Submit the doc op but DO NOT execute it locally in the DOM, let the browser editor do it.
    this.blockViewUpdates = true;
    this.submitMutation(mut);
    this.blockViewUpdates = false;
};

LW.Editor.prototype.keyup = function(e) {
    // window.console.log("KeyUp = " + e.keyIdentifier.toString() + " code=" + e.keyCode.toString());
        
    // Cursor keys are handled here. Everything else passes.
    if ( e.keyCode < 33 || e.keyCode > 40 )
        return;
        
    var sel = window.getSelection();
    var selDom = sel.focusNode;
    var selOffset = sel.focusOffset;
        
    // Is the cursor inside a caret?
    var caret;
    if ( selDom.nodeType == 1 && selDom.className == "jsot_caret" )
        caret = selDom;
    else if ( selDom.nodeType == 3 && selDom.parentNode.className == "jsot_caret" )
        caret = selDom.parentNode;
        
    // Cursor is on a caret? Then move it away
    if ( caret )
    {
        // Left, Home, PageUp, Up
        if ( e.keyCode == 37 || e.keyCode == 36 || e.keyCode == 33 || e.keyCode == 38 )
        {
            // Skip all carets to the left
            while ( caret.previousSibling && caret.previousSibling.className == "jsot_caret" )
                caret = caret.previousSibling;
            // Position the cursor left of it.
            if ( caret.previousSibling )
            {
                // Left of the caret there is a span with a text node
                selDom = caret.previousSibling.lastChild;
                selOffset = caret.previousSibling.lastChild.data.length;
            }
            else
            {
                selDom = caret;
                selOffset = 0;
            }
        }
        else
        {
            // Skip all carets to the right
            while ( caret.nextSibling && caret.nextSibling.className == "jsot_caret" )
                caret = caret.nextSibling;
            // Position the cursor left of it.
            if ( caret.nextSibling )
            {
                // Right of the caret there is a span with a text node
                selDom = caret.nextSibling.firstChild;
                selOffset = 1;
            }
            else
            {
                // TODO: Move the cursor to the left of the caret instead!
                selDom = caret;
                selOffset = 0;
            }
        }
        
        sel.collapse( selDom, selOffset );
    }
};

/**
 * TODO!!!
 * Changes the style of the current selection.
 *
 * @param {string} styleKey defines the style, e.g. "style/fontWeight"
 * @param {string} styleValue defines the value for the style, e.g. "bold".
 */
/*LW.Editor.prototype.setSelectionStyle = function(styleKey, styleValue)
{
    var sel = window.getSelection();
    var selDom = sel.anchorNode;
    var selOffset = sel.anchorOffset;
        
    if ( sel.isCollapsed )
        return;

    var pos1 = this.getTextPosition( sel.anchorNode, sel.anchorOffset );
    var pos2 = this.getTextPosition( sel.focusNode, sel.focusOffset );

    if ( pos2.lineno < pos1.lineno || ( pos2.lineno == pos1.lineno && pos2.charCount < pos1.charCount ) )
    {
        var tmp = pos2;
        pos2 = pos1;
        pos1 = tmp;
    }
    
    // TODO: create and submit doc op
};
*/

LW.Editor.prototype.deleteSelection = function(showCursor) {
    var sel = window.getSelection();
    var selDom = sel.anchorNode;
    var selOffset = sel.anchorOffset;
    
    // Paranoia
    if ( sel.isCollapsed )
        throw "Expected a selection";

    var pos1 = this.getTextPosition( sel.anchorNode, sel.anchorOffset );
    var pos2 = this.getTextPosition( sel.focusNode, sel.focusOffset );

    if ( pos2.paragIndex < pos1.paragIndex || ( pos2.paragIndex == pos1.paragIndex && pos2.charCount < pos1.charCount ) )
    {
        var tmp = pos2;
        pos2 = pos1;
        pos1 = tmp;
    }

    // Create and submit doc op
    var mut = [];
    var count = this.otCharCountTo_(pos1);
    if ( count > 0 ) {
        mut.push({"$s": count});
    }
    mut.push({"$d": this.otCharCountFromTo_(pos1, pos2)});
    count = this.otCharCountFrom_(pos2);
    if ( count > 0 ) {
        mut.push({"$s": count});
    }
    this.submitMutation(mut);

    if ( showCursor ) {
        var domPos = this.getDomPosition(pos1);
        sel.collapse( domPos.element, domPos.index );
    }
};

/**
 * Internal helper function.
 *
 * @return the number of characters in a HTML node.
 *
 * This helper function is required to map the cursor position from inside the dom
 * to a line/charCount representation.
 */
LW.Editor.prototype.domCharCount = function( node ) {
    if ( node.nodeType == 3 )
        return node.data.length;
    if ( node.className == "jsot_caret" )
        return 0;
    var result = 0;
    var c = node.firstChild;
    while( c ) {
        result += this.domCharCount(c);
        c = c.nextSibling;
    }
    return result;
};

/**
 * Internal helper function.
 *
 * Determines the line number and character position inside the line
 * from a given HTML node and offset. This helps in mapping HTML cursor positions
 * to a position in JSOT.Doc.
 *
 * The counterpart is getDomPosition
 */
LW.Editor.prototype.getTextPosition = function(selDom, selOffset) {
    var charCount = 0;
    // In a text node?
    if ( selDom.nodeType == 3 )
        charCount = selOffset;
    // At the end of a HTML node?
    else if ( selOffset == 1 )
        charCount = this.domCharCount(selDom);
    // Count the number of characters in front of selDom and find the line DIV
    var line = selDom;
    while( line.nodeType == 3 || line.className != "lw_line" ) {
        var p = line.previousSibling;
        while( p ) {
            charCount += this.domCharCount(p);
            p = p.previousSibling;
        }
        line = line.parentNode;
    }
    // Find the line number
    var lineno = 0;
    var l = line.previousSibling;
    while( l ) {
        lineno++;
        l = l.previousSibling;
    }
        
    return { paragIndex : lineno, charCount : charCount };
};

/**
 * Internal helper function.
 *
 * Determines the HTML element and offset from a given line, line number and character count inside the line.
 *
 * The counterpart is getLinePosition
 */
LW.Editor.prototype.getDomPosition = function( pos ) {
    var it = new LW.Editor.DomIterator(this, pos);
    return { element : it.current, index : it.index };
};

/**
 * Internal helper function.
 *
 * Sets the HTML cursor according to 'this.cursor'.
 */
// TODO
LW.Editor.prototype.showCursor = function()
{
        var sel = window.getSelection();
        
        // Show a selection range?
        if ( this.cursorRange )
        {
                var start = this.getDomPosition( this.cursorRange.focus.line, this.cursorRange.focus.lineno, this.cursorRange.focus.charCount );
                var end = this.getDomPosition( this.cursorRange.anchor.line, this.cursorRange.anchor.lineno, this.cursorRange.anchor.charCount );
                // The cursor is at the beginning of the selection?
                if ( this.cursorRange.focus.lineno != this.cursor.lineno || this.cursorRange.focus.charCount != this.cursor.charCount )
                {
                        // Something is wrong ...
                        window.console.log("Selection is wrong");
                        delete this.cursorRange;
                        this.showCursor();
                }
                sel.collapse( end.node, end.offset );
                sel.extend( start.node, start.offset );
        }
        // Simply show the cursor
        else
        {
                var cursorPos = this.getDomPosition( this.cursor.line, this.cursor.lineno, this.cursor.charCount );
                sel.collapse( cursorPos.node, cursorPos.offset );
        }
};

LW.Editor.prototype.renderParagraph = function(paragIndex, insert) {
    var parag = this.entity.paragraphs[paragIndex];
    var html;
    if ( parag.text.length == 0 ) {
        html = '<br>';
    } else {
        // TODO: Format
        html = esc(parag.text);
    }
    if ( insert ) {
        var dom = document.createElement("p");
        dom.className = "lw_line";
        dom.innerHTML = html;
        this.dom.insertBefore(dom, this.dom.children[paragIndex]);
    } else {
        this.dom.children[paragIndex].innerHTML = html;
    }
};

LW.Editor.prototype.viewRenderParagraph = function(paragIndex) {
    if ( this.blockViewUpdates ) {
        return;
    }
    this.renderParagraph(paragIndex, false);
};

LW.Editor.prototype.viewInsertParagraph = function(paragIndex) {
    if ( this.blockViewUpdates ) {
        return;
    }
    this.renderParagraph(paragIndex, true);
};

LW.Editor.prototype.viewDeleteParagraph = function(paragIndex) {
    if ( this.blockViewUpdates ) {
        return;
    }
    this.dom.removeChild(this.dom.children[paragIndex]);    
};

LW.Editor.prototype.otCharCountTo_ = function(pos) {
    var count = 0;
    for( var i = 0; i < pos.paragIndex; ++i ) {
        count += this.entity.paragraphs[i].text.length + 1;
    }
    count += pos.charCount;
    var tombStream = new lightwave.ot.TombStream(this.entity.tombs);
    var tmp = tombStream.SkipChars(count);
    if (tmp[1]) {
        console.log("ERR: SkipChars 1");
    }
    return tmp[0];
};

LW.Editor.prototype.otCharCountFrom_ = function(pos) {
    var count = 0;
    for( var i = 0; i < pos.paragIndex; ++i ) {
        count += this.entity.paragraphs[i].text.length + 1;
    }
    count += pos.charCount;
    var tombStream = new lightwave.ot.TombStream(this.entity.tombs);
    var tmp = tombStream.SkipChars(count);
    if (tmp[1]) {
        console.log("ERR: SkipChars 2");
    }
    return tombStream.SkipToEnd();
};

LW.Editor.prototype.otCharCountFromTo_ = function(pos1, pos2) {
    var count = 0;
    for( var i = 0; i < pos1.paragIndex; ++i ) {
        count += this.entity.paragraphs[i].text.length + 1;
    }
    count += pos1.charCount;
    var tombStream = new lightwave.ot.TombStream(this.entity.tombs);
    var tmp = tombStream.SkipChars(count);
    if (tmp[1]) {
        console.log("ERR: SkipChars 3");
    }

    var count = 0;
    if ( pos1.paragIndex == pos2.paragIndex ) {
        count = pos2.charCount - pos1.charCount;
    } else {
        count = this.entity.paragraphs[pos1.paragIndex].text.length - pos1.charCount;
        for( var i = pos1.paragIndex + 1; i < pos2.paragIndex; ++i ) {
            count += this.entity.paragraphs[i].text.length + 1;
        }
        count += pos2.charCount;
    }
    var tmp = tombStream.SkipChars(count);
    if (tmp[1]) {
        console.log("ERR: SkipChars 4");
    }

    return tmp[0];
};

LW.Editor.prototype.submitMutation = function(mut) {
    console.log("OP: " + JSON.stringify(mut));
    op = {"$t": mut};
    // Apply locally
    lightwave.ot.ExecuteOperation(this.entity, op);
    // Send mutation to the server
    var msg = {perma: this.entity.page.pageBlobRef, op: op, type: "mutation", entity: this.entity.id};
    var pageContent = this.entity;
    store.submit(msg, null, null, function(m) { m.entity = pageContent.id });
};

// TODO
function esc(str) {
    return str;
}

// ------------------------------------------------------
// DomIterator

/**
 * This class is used by the editor to iterate over and modify the HTML content in response
 * to user actions or applied deltas.
 *
 * @param {HTMLElement} dom is the HTML element that is contentEditable=true.
 *
 * @constructor
 */
LW.Editor.DomIterator = function( editor, pos ) {
    /**
     * The HTML div element that hosts the editor.
     * @type {HTMLElementDiv}
     */
    this.editor = editor;
    /**
     * Number of the current line.
     */
    this.paragIndex = -1;
    this.parag = null;
    /**
     * The HTMLElementP of the current line or null at the beginning.
     * @type {HTMLElementP}
     */
    this.line = null;
    /**
     * Number of characters inside the line
     */
    this.charCount = 0;
    /**
     * A span or text node or null. A value of null means the position at the end of the current line
     * @type {HTMLElementSpan} or {HTMLElementP} or {HTMLTextNode}
     */
    this.current = null;
    /**
     * Position inside a text node. Only relevant when this.current is a text node.
     */
    this.index = 0;

    if ( pos ) {
        this.paragIndex = pos.paragIndex;
        this.parag = this.editor.entity.paragraphs[pos.paragIndex];
        this.line = this.editor.dom.children[pos.paragIndex];       
        this.index = 0;
        this.current = this.line;
        this.charCount = 0;
        this.skipChars( pos.charCount );
    }
};

/**
 * Moves the iterator over a number of characters inside a line.
 */
LW.Editor.DomIterator.prototype.skipChars = function( count ) {
    if ( count == 0 )
        return;
    if ( this.paragIndex == -1 )
        throw "Must skip line break first";
    if ( !this.current )
        throw "Cannot skip characters at the end of a line";
    if ( this.charCount + count > this.parag.text.length ) {
        throw "Skipping behind the end of a line";
    }

    // If inside a SPAN/P go down to the text node
    while ( this.current.nodeType == 1 ) {
        this.current = this.current.firstChild;
        // Detected an empty span?
        if ( !this.current )
            throw "Detected an empty span/p"
    }
                
    var min = Math.min( this.current.data.length - this.index, count );
    this.index += min;
    this.charCount += min;
    count -= min;

    // Need to skip more characters? -> recursion
    if ( count > 0 ) {
        this.current = this.current.parentNode.nextSibling;
        this.index = 0;
        this.skipChars( count, format );
    }
};
