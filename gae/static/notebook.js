function Book(id, text) {
    this.id = id;
    this.text = text;
    // An array of Chapter objects
    this.chapters = [];
    this.currentChapter = null;

    this.addChapter( new Chapter(this, "inbox", "Inbox", 0, null), true);
    this.setActiveChapter(this.chapters[0]);
}

function Chapter(book, id, text, colorScheme, after) {
    this.book = book;
    this.id = id;
    this.text = text;
    this.after = after;
    // An array of Page objects
    this.pages = [];
    this.colorScheme = colorScheme;
    this.tab = null;
    this.currentPage = null;
}

function Page(chapter, id, text, after) {
    this.chapter = chapter;
    this.id = id;
    this.text = text;
    this.after = after;
    this.vtab = null;
    // An array of PageContent objects
    this.contents = [];
    // An array of Follower objects
    this.followers = [];
    // An array of Follower objects
    this.invitations = [];
}

function PageContent(page, id, text, layout) {
    this.page = page;
    this.id = id;
    this.text = text;
    this.layout = layout;
    this.paragraphs = [];
    this.tombs = [text.length];
    this.buildParagraphs();
}

function Follower(page, userid, username) {
    this.page = page;
    this.id = userid;
    this.name = username;
    if (!this.name) {
        this.name = this.id.substr(0, this.id.indexOf("@"));
    }
}

Book.prototype.getChapter = function(id) {
    for( var i = 0; i < this.chapters.length; i++) {
        if (this.chapters[i].id == id) {
            return this.chapters[i];
        }
    }
    return null;
};

Book.prototype.addChapter = function(c, make_active) {
    // Determine where to insert the chapter
    var pos = 0;
    if (c.after) {
        for( var i = 0; i < this.chapters.length; i++) {
            var x = this.chapters[i];
            if (x.id == c.after) {
                pos = i + 1;
                break;
            }
        }
    }
    var to_pos = pos;
    for( var i = pos; i < this.chapters.length; i++) {
        var x = this.chapters[i];
        if (x.id != c.after) {
            break;
        }
        to_pos++;
    }
    for( var i = pos; i < to_pos; i++) {
        var x = this.chapters[i];
        if (x.id < id) {
            pos++;
        } else {
            break;
        }
    }
    this.chapters.splice(pos, 0, c);
    // Insert tab
    var tab = document.createElement("div");
    c.tab = tab;
    if (make_active) {
        tab.className = "tab tab" + c.colorScheme.toString() + " activetab";
    } else {
        tab.className = "tab tab" + c.colorScheme.toString() + " inactivetab";
    }
    tab.appendChild(document.createTextNode(c.text));
    var book = this;
    tab.addEventListener("click", function() { book.setActiveChapter(c); }) 
    var tabs = document.getElementById("tabs");
    tabs.insertBefore(tab, tabs.children[pos]);
    if (make_active) {
        this.setActiveChapter(c);
    } else {
        this.setActiveChapter(this.currentChapter);
    }
};

Book.prototype.setActiveChapter = function(chapter) {
    // Deactivate a tab (if one is active)
    var c = this.currentChapter;
    // Shift all tabs on the right (including the '+' tab) ....
    var tabs = document.getElementById("tabs");
    for( var i = 0; i < tabs.children.length; i++) {
        var t = tabs.children[i];
        t.style.left = (-i * 3).toString() + "px";
        t.style.zIndex = 99 - i;
    }
    if (c == chapter) {
        return;
    }
    var stack = document.getElementById("stack");
    if (c) {
        $(c.tab).removeClass("activetab");
        $(c.tab).addClass("inactivetab");
        $(stack).removeClass("stack" + c.colorScheme.toString());
        var vtabs = document.getElementById("vtabs");
        for( ; vtabs.children.length > 1;) {
            vtabs.removeChild( vtabs.children[1] );
        }
        for( var i = 0; i < c.pages.length; i++ ) {
            delete c.pages[i].tab;
        }
    }
    this.currentChapter = chapter;
    if (!chapter) {
        return
    }
    $(chapter.tab).removeClass("inactivetab");
    $(chapter.tab).addClass("activetab");
    chapter.tab.style.zIndex = 100;
    $(stack).addClass("stack" + chapter.colorScheme.toString());
    // Create pages
    var vtabs = document.getElementById("vtabs");
    for( var i = 0; i < chapter.pages.length; i++) {
        var p = chapter.pages[i];
        var vtab = document.createElement("div");
        p.vtab = vtab;
        vtab.className = "vtab inactivevtab" + chapter.colorScheme.toString();
        vtab.appendChild(document.createTextNode(p.text));
        vtab.addEventListener("click", function(x) { return function() { chapter.setActivePage(x); }; }(p) ); 
        vtabs.appendChild(vtab);
    }
    var p = chapter.currentPage;
    if (!p && chapter.pages.length > 0) {
        p = chapter.pages[0];
    }
    if (p) {
        chapter.currentPage = null;
        chapter.setActivePage(p);
    }
    if (chapter.id == "inbox") {
        document.getElementById("newvtab").style.visibility = "hidden";
    } else {
        document.getElementById("newvtab").style.visibility = "visible";
    }
};

Chapter.prototype.addPage = function(p, make_active) {
    // Determine where to insert the chapter
    var pos = 0;
    if (p.after) {
        for( var i = 0; i < this.pages.length; i++) {
            var x = this.pages[i];
            if (x.id == p.after) {
                pos = i + 1;
                break;
            }
        }
    }
    var to_pos = pos;
    for( var i = pos; i < this.pages.length; i++) {
        var x = this.pages[i];
        if (x.id != p.after) {
            break;
        }
        to_pos++;
    }
    for( var i = pos; i < to_pos; i++) {
        var x = this.pages[i];
        if (x.id < id) {
            pos++;
        } else {
            break;
        }
    }
    this.pages.splice(pos, 0, p);
    if (!$(this.tab).hasClass("activetab")) {
        return
    }
    // Do not update the UI if the chapter is not visible at all
    if (this.book.currentChapter != this ) {
        return;
    }
    // Insert vtab
    var vtab = document.createElement("div");
    p.vtab = vtab;
    if (make_active) {
        vtab.className = "vtab activevtab";
    } else {
        vtab.className = "vtab inactivevtab" + this.colorScheme.toString();
    }
    vtab.appendChild(document.createTextNode(p.text));
    var chapter = this;
    vtab.addEventListener("click", function() { chapter.setActivePage(p); }) 
    var vtabs = document.getElementById("vtabs");
    vtabs.insertBefore(vtab, vtabs.children[pos + 1]);
    if (make_active) {
        this.setActivePage(p);
    } else {
        this.setActivePage(this.currentPage);
    }
};

Chapter.prototype.setActivePage = function(page) {
    console.log("ACTIVE PAGE " + page.pageBlobRef);
    // Deactivate a tab (if one is active)
    var p = this.currentPage;
    // Shift all tabs on the right (including the '+' tab) ....
    var vtabs = document.getElementById("vtabs");
    for( var i = 1; i < vtabs.children.length; i++) {
        var t = vtabs.children[i];
        t.style.top = (-i + 1).toString() + "px";
    }
    if (p == page) {
        return;
    }
    if (p) {
        $(p.vtab).removeClass("activevtab");
        $(p.vtab).addClass("inactivevtab" + this.colorScheme.toString());
    }
    $(page.vtab).removeClass("inactivevtab" + this.colorScheme.toString());
    $(page.vtab).addClass("activevtab");
    var pagediv = document.getElementById("page");
    var content = document.getElementsByClassName("content");
    for( ; content.length > 0; ) {
        pagediv.removeChild(content[0]);
    }
    var sharediv = document.getElementById("share");
    var friends = document.getElementsByClassName("friend");
    for( ; friends.length > 0; ) {
        sharediv.removeChild(friends[0]);
    }
    page.showContents();
    page.showFollowers();
    if (store) {
        if (this.currentPage && this.currentPage.pageBlobRef) {
            store.close(this.currentPage.pageBlobRef);
        }
        if (page.pageBlobRef.substring(0,4) != "tmp-") {
            store.openPage(page);
        }
    }
    this.currentPage = page;
};

Page.prototype.addContent = function(content) {
    this.contents.push(content);
    if (!this.isVisible()) {
        return;
    }
    this.showContent(content);
};

Page.prototype.showContents = function() {
    for( var i = 0; i < this.contents.length; i++ ) {
        this.showContent(this.contents[i]);
    }
};

Page.prototype.showContent = function(content) {
    var div;
    if (content.layout == "title") {
        var pagediv = document.getElementById("page");
        div = document.createElement("div");
        div.className = "content title";
        div.contentEditable = true;
        div.appendChild(document.createTextNode(content.text));
        pagediv.appendChild(div);
        var div2 = document.createElement("div");
        div2.className = "content date";
        div2.innerHTML = "Sunday, June 30, 2011<br>23:56";
        pagediv.appendChild(div2);        
    } else if (content.layout == "textbox") {
        var pagediv = document.getElementById("page");
        div = document.createElement("div");
        div.className = "content textbox";
        div.contentEditable = true;
        div.appendChild(document.createTextNode(content.text));
        pagediv.appendChild(div);
    } else {
        console.log("UNKNOWN layout")
        return;
    }
    var editor = new LW.Editor(content, div);
};

Page.prototype.addFollower = function(follower) {
    this.followers.push(follower);
    // Remove from invitations
    for( var i = 0; i < this.invitations.length; i++ ) {
        if (this.invitations[i].id == follower.id) {
            this.invitations.splice(i, 1);
            if (this.isVisible()) {
                var div = document.getElementById("friend-" + follower.id);
                if (div ) {
                    var sharediv = document.getElementById("share");
                    sharediv.removeChild(div);
                }
            }
            break;
        }
    }
    if (!this.isVisible()) {
        return;
    }
    this.showFollower(follower, false);
};

Page.prototype.getFollower = function(userid) {
    for( var i = 0; i < this.followers.length; i++ ) {
        if (this.followers[i].id == userid) {
            return this.followers[i];
        }
    }
    return null;
};

Page.prototype.addInvitation = function(follower) {
    this.invitations.push(follower);
    if (!this.isVisible()) {
        return;
    }
    this.showFollower(follower, true);
};

Page.prototype.getInvitation = function(userid) {
    for( var i = 0; i < this.invitations.length; i++ ) {
        if (this.invitations[i].id == userid) {
            return this.invitations[i];
        }
    }
    return null;
};

Page.prototype.showFollowers = function() {
    for( var i = 0; i < this.followers.length; i++ ) {
        this.showFollower(this.followers[i], false);
    }
    for( var i = 0; i < this.invitations.length; i++ ) {
        this.showFollower(this.invitations[i], true);
    }
};

Page.prototype.showFollower = function(follower, inviteOnly) {
    var div = document.createElement("div");
    div.id = "follower-" + follower.id;
    // HACK
    if (!inviteOnly)
        div.className = "friend friendonline";
    else
        div.className = "friend friendaway";
    var img = document.createElement("img");
    img.className = "friend-image";
    img.src = "unknown.png";
    div.appendChild(img);
    var div2 = document.createElement("div");
    var span = document.createElement("span");
    span.className = "friend-name";
    span.innerText = follower.name;
    div2.appendChild(span);
    div2.appendChild(document.createElement("br"));
    var span = document.createElement("span");
    span.className = "friend-id";
    span.innerText = follower.id;
    div2.appendChild(span);    
    div.appendChild(div2);
    var sharediv = document.getElementById("share");
    var invitesdiv = document.getElementById("invitations");
    if (inviteOnly) {
        sharediv.appendChild(div);
    } else {
        var sharediv = document.getElementById("share");
        sharediv.insertBefore(div, invitesdiv);
    }
};

Page.prototype.isVisible = function(content) {
    if (this.chapter.currentPage != this) {
        return false;
    }
    if (this.chapter.book.currentChapter != this.chapter) {
        return false;
    }
    return true;
};

Page.prototype.getContent = function(id) {
    for( var i = 0; i < this.contents.length; i++) {
        if (this.contents[i].id == id) {
            return this.contents[i];
        }
    }
    return null;
};

PageContent.prototype.mutate = function(mutation) {
    lightwave.ot.ExecuteOperation(this, mutation.op);
};

PageContent.prototype.buildParagraphs = function() {
    this.paragraphs = [];
    var parags = this.text.split("\n");
    for( var i = 0; i < parags.length; i++ ) {
        var p = parags[i];
        this.paragraphs.push({text:p});
    }
};

PageContent.prototype.Begin = function() {
    this.tombStream = new lightwave.ot.TombStream(this.tombs);
    this.mut_charCount = 0;
    this.mut_paragIndex = 0;
    this.mut_paragModified = false;
};

PageContent.prototype.InsertChars = function(str) {
    this.tombStream.InsertChars(str.length);
    var parags = str.split("\n");
    for( var i = 0; i < parags.length; i++ ) {
        var s = parags[i];
        var parag = this.paragraphs[this.mut_paragIndex];
        if ( i > 0 ) {
            this.paragraphs.splice(this.mut_paragIndex + 1, 0, {text:s + parag.text.substring(this.mut_charCount, parag.text.length)});
            parag.text = parag.text.substring(0, this.mut_charCount);
            if ( this.listener ) {
                this.listener.viewRenderParagraph(this.mut_paragIndex);
                this.listener.viewInsertParagraph(this.mut_paragIndex + 1);
            }   
            this.mut_paragIndex++;
            this.mut_charCount = s.length;
            this.mut_paragModified = false;
        } else {
            parag.text = parag.text.substring(0, this.mut_charCount) + s + parag.text.substring(this.mut_charCount, parag.text.length);
            this.mut_charCount += s.length;
            this.mut_paragModified = true;
        }
    }
};

PageContent.prototype.InsertTombs = function(count) {
    this.tombStream.InsertTombs(count);
};

PageContent.prototype.Delete = function(count) {
    var burried, err;
    var result = this.tombStream.Bury(count);
    burried = result[0];
    err = result[1];
    if (err) {
        return err;
    }
    while( burried > 0 ) {
        this.mut_paragModified = true;
        var parag = this.paragraphs[this.mut_paragIndex];
        // Delete a line break?
        if (this.mut_charCount == parag.text.length) {
            parag.text = parag.text + this.paragraphs[this.mut_paragIndex + 1].text;
            this.paragraphs.splice(this.mut_paragIndex + 1, 1);
            burried--;
            if (this.listener) {
                this.listener.viewDeleteParagraph(this.mut_paragIndex + 1);
            }
        } else {
            var l = Math.min(burried, parag.text.length - this.mut_charCount);
            burried -= l;
            parag.text = parag.text.substring(0, this.mut_charCount) + parag.text.substring(this.mut_charCount + l, parag.text.length);
        }
        if (this.mut_paragIndex >= this.paragraphs.length) {
            throw "Error in delete";
        }
    }
    return null;
};

PageContent.prototype.Skip = function(count) {
    var chars = 0, err;
    var result = this.tombStream.Skip(count);
    chars = result[0];
    err = result[1];
    if (err) {
        return err;
    }
    while( chars > 0 ) {
        var parag = this.paragraphs[this.mut_paragIndex];
        // Skip a line break?
        if (this.mut_charCount == parag.text.length) {
            if (this.mut_paragModified) {
                if (this.listener) {
                    this.listener.viewRenderParagraph(this.mut_paragIndex);
                }
                this.mut_paragModified = false;
            }
            this.mut_paragIndex++;
            this.mut_charCount = 0;
            chars--;
        } else {
            var l = Math.min(chars, parag.text.length - this.mut_charCount);
            chars -= l;
            this.mut_charCount += l;
        }
        if (this.mut_paragIndex >= this.paragraphs.length) {
            throw "Error in skip";
        }
    }
    return null;
};

PageContent.prototype.End = function() {
    if (this.listener && this.mut_paragModified) {
        this.listener.viewRenderParagraph(this.mut_paragIndex);
    }
    delete this.tombStream;
}
