function Book(id, text) {
    this.id = id;
    this.text = text;
    // An array of Chapter objects
    this.chapters = [];
    this.currentChapter = null;

    this.inbox = new Chapter(this, "inbox", "Inbox", 0, null);
    this.addChapter(this.inbox, true);
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
    this.nextSeq = 0;
    this.layout = null;
    // An array of PageContent objects
    this.contents = [];
    // An array of Follower objects
    this.followers = [];
    // An array of Follower objects
    this.invitations = [];
}

function PageContent(page, id, text, cssClass, style) {
    this.page = page;
    this.id = id;
    this.text = text;
    this.cssClass = cssClass;
    this.style = style;
    this.paragraphs = [];
    this.tombs = [text.length];
    this.buildParagraphs();
}

function PageLayout(page, id, style) {
    this.page = page;
    this.id = id;
    this.style = style;
}

function Follower(page, userid, username) {
    this.page = page;
    this.id = userid;
    this.name = username;
    if (!this.name) {
        this.name = this.id.substr(0, this.id.indexOf("@"));
    }
}

// Returns an entity based on its entity blobref
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
    // Show the chapter-tab in the UI
    c.renderTab();
    var tabs = document.getElementById("tabs");
    tabs.insertBefore(c.tab, tabs.children[pos]);
    this.positionChapterTabs_();
    if (make_active) {
        this.setActiveChapter(c);
    }
};

Book.prototype.positionChapterTabs_ = function() {
    // Shift all tabs on the right (including the '+' tab) ....
    var tabs = document.getElementById("tabs");
    for( var i = 0; i < tabs.children.length; i++) {
        var t = tabs.children[i];
        t.style.left = (-i * 3).toString() + "px";
        t.style.zIndex = 99 - i;
    }
};

Book.prototype.setActiveChapter = function(chapter) {
    if (chapter == this.currentChapter) {
        return;
    }
    // Deactivate a tab (if one is active)
    if (this.currentChapter) {
        this.currentChapter.close();
    }
    this.currentChapter = chapter;
    if (this.currentChapter) {
        this.currentChapter.open();
    }
};

Book.prototype.getPages = function() {
    var result = { };
    for (var i = 0; i < this.chapters.length; i++) {
        var chapter = this.chapters[i];
        for( var k = 0; k < chapter.pages.length; k++) {
            var page = chapter.pages[k];
            if (result[page.pageBlobRef]) {
                result[page.pageBlobRef] = result[page.pageBlobRef].concat(chapter);
            } else {
                result[page.pageBlobRef] = [chapter];
            }
        }
    }
    return result;
};

Book.prototype.setUnreadInfo = function(unread) {
    for (var i = 0; i < this.chapters.length; i++) {
        var chapter = this.chapters[i];
        chapter.setUnreadInfo(unread);
    }
};

Book.prototype.setPageUnread = function(perma_blobref, unread) {
    for (var i = 0; i < this.chapters.length; i++) {
        var chapter = this.chapters[i];
        chapter.setPageUnread(perma_blobref, unread);
    }
};

// ============================================================================
// Chapter
// ============================================================================

Chapter.prototype.open = function() {
    $(this.tab).removeClass("inactivetab");
    $(this.tab).addClass("activetab");
    this.tab.style.zIndex = 100;
    var stack = document.getElementById("stack");
    $(stack).addClass("stack" + this.colorScheme.toString());
    // Show a certain page in the inbox?
    if (this.id == "inbox") {
        if (!this.currentPage) {
            this.renderInbox_();
        }
    } else {
        // Create vtabs for pages
        var vtabs = document.getElementById("vtabs");
        for( var i = 0; i < this.pages.length; i++) {
            var p = this.pages[i];
            p.renderTab();
            vtabs.appendChild(p.vtab);
        }
        this.positionTabs_();
        if (!this.currentPage && this.pages.length > 0) {
            this.currentPage = this.pages[0];
        }
    }
    this.showUIElements_();
    if (this.currentPage) {
        this.currentPage.open();
    }
};

Chapter.prototype.close = function() {
    var stack = document.getElementById("stack");
    $(this.tab).removeClass("activetab");
    $(this.tab).addClass("inactivetab");
    $(stack).removeClass("stack" + this.colorScheme.toString());
    if (this.id == "inbox") {
        this.closeInbox_();
    } else {
        // Remove all vtabs
        var vtabs = document.getElementById("vtabs");
        for( ; vtabs.children.length > 1;) {
            vtabs.removeChild( vtabs.children[1] );
        }
        for( var i = 0; i < this.pages.length; i++ ) {
            delete this.pages[i].tab;
        }
    }
    if (this.currentPage) {
        this.currentPage.close();
    }
};

Chapter.prototype.getPageByPageBlobRef = function(blobref) {
    for( var i = 0; i < this.pages.length; i++) {
        if (this.pages[i].pageBlobRef == blobref) {
            return this.pages[i];
        }
    }
    return null;
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
    // Perhaps the page is unread? -> show it
    this.updateUnreadPagesCount();
    // Do not update the UI if the chapter is not visible at all
    if (this.book.currentChapter != this ) {
        return;
    }
    // Inbox?
    if ( this.id == "inbox") {
        if (!this.currentPage) {
            var div = this.renderInboxItem_(p);
            var inboxdiv = document.getElementById("inbox");
            inboxdiv.insertBefore(div, inboxdiv.children[1]);
        }
        return;
    }
    // Insert vtab
    p.renderTab();
    var vtabs = document.getElementById("vtabs");
    vtabs.insertBefore(p.vtab, vtabs.children[pos + 1]);
    this.positionTabs_();
    if (make_active) {
        this.setActivePage(p);
    }
};

Chapter.prototype.removePage = function(page) {
    var index = 0;
    for ( var i = 0; i < this.pages.length; i++) {
        if (this.pages[i] == page) {
            break;
        }
        index++;
    }
    if ( this.currentPage == page ) {
        if (this.id == "inbox") {
            this.setActivePage(null);
        } else if (this.pages.length > index + 1) {
            this.setActivePage(this.pages[index + 1]);
        } else if (index > 0) {
            this.setActivePage(this.pages[index - 1]);
        } else {
            this.setActivePage(null);
        }
    }
    if (page.vtab) {
        page.vtab.parentNode.removeChild(page.vtab);
        delete page.vtab;
    }
    if (page.inbox_div) {
        page.inbox_div.parentNode.removeChild(page.inbox_div);
        delete page.inbox_div;
    }
    this.pages.splice(index, 1);
};

Chapter.prototype.positionTabs_ = function() {
    // Shift all tabs on the right (including the '+' tab) ....
    var vtabs = document.getElementById("vtabs");
    for( var i = 1; i < vtabs.children.length; i++) {
        var t = vtabs.children[i];
        t.style.top = (-i + 1).toString() + "px";
    }
};

Chapter.prototype.showUIElements_ = function() {
    if (this.id == "inbox") {
        if (this.currentPage) {
            document.getElementById("back-to-inbox").style.display = "block";
            document.getElementById("inbox").style.display = "none";
            document.getElementById("pagecontainer").style.display = "block";
        } else {
            document.getElementById("inbox").style.display = "block";
            document.getElementById("pagecontainer").style.display = "none";
        }
        document.getElementById("newvtab").style.visibility = "hidden";
    } else {
        document.getElementById("back-to-inbox").style.display = "none";
        document.getElementById("newvtab").style.visibility = "visible";
        document.getElementById("inbox").style.display = "none";
        document.getElementById("pagecontainer").style.display = "block";
    }
};

Chapter.prototype.setActivePage = function(page) {
    if (this.currentPage == page) {
        return;
    }
    if (this.currentPage) {
        this.currentPage.close();
    } else if (this.id == "inbox") {
        this.closeInbox_();
    }
    this.currentPage = page;
    this.showUIElements_();
    if (this.currentPage) {
        this.currentPage.open();
    } else if (this.id == "inbox") {
        this.renderInbox_();
    }
};

// Creates the horizontal tab for the chapter.
Chapter.prototype.renderTab = function() {
    // Insert tab
    this.tab = document.createElement("div");
    if (this.book.currentChapter == this) {
        this.tab.className = "tab tab" + this.colorScheme.toString() + " activetab";
    } else {
        this.tab.className = "tab tab" + this.colorScheme.toString() + " inactivetab";
    }
    this.tab.appendChild(document.createTextNode(this.text));
    this.showUnreadPagesCount();
    var chapter = this;
    this.tab.addEventListener("click", function() {
        store.waitForPageIO( function() {
            chapter.book.setActiveChapter(chapter);
        });
    });
};

Chapter.prototype.closeInbox_ = function(page) {
    var inboxdiv = document.getElementById("inbox");
    var items = document.getElementsByClassName("inboxitem");
    for( ; items.length > 0; ) {
        inboxdiv.removeChild(items[0]);
    }
};

Chapter.prototype.renderInbox_ = function(page) {
    var inboxdiv = document.getElementById("inbox");
    var prev;
    for (var i = 0; i < this.pages.length; i++) {
        var div = this.renderInboxItem_(this.pages[i]);
        inboxdiv.insertBefore(div, prev);
        prev = div;
    }
};

Chapter.prototype.redrawInboxItem = function(page) {
    if (this.book.currentChapter != this || this.currentPage) {
        return;
    }
    var div = document.getElementById("inboxitem-" + page.pageBlobRef);
    this.renderInboxItem_(page, div);
};

Chapter.prototype.renderInboxItem_ = function(page, div) {
    var isnew = true;
    if (div) {
        isnew = false;
        div.innerHTML = "";
    } else {
        div = document.createElement("div");
        page.inbox_div = div;
    }
    div.id = "inboxitem-" + page.pageBlobRef;
    if (page.inbox_latestauthors.length > 0) {
        div.className = "inboxitem inboxitemnew";
    } else {
        div.className = "inboxitem";
    }
    var input = document.createElement("input");
    input.className = "inboxcheckbox";
    input.type = "checkbox";
    input.addEventListener("click", function(e) {
        if (!e) var e = window.event;
 	e.cancelBubble = true;
	if (e.stopPropagation) e.stopPropagation();
        var targ;
	if (e.target) targ = e.target;
	else if (e.srcElement) targ = e.srcElement;
        if (targ && targ.checked) {
            page.inbox_selected = true;
            $(div).addClass("inboxitemselected");
        } else {
            page.inbox_selected = false;
            $(div).removeClass("inboxitemselected");
        }
    });
    div.appendChild(input);
    var span = document.createElement("span");
    span.className = "inboxauthor";
    var authors = [];
    for (var i = 0; i < page.inbox_latestauthors.length; i++) {
        // TODO: HTML escape
        authors.push("<b>" +  page.inbox_latestauthors[i] + "</b>")
    }
    for (var i = 0; i < page.inbox_authors.length; i++) {
        // TODO: HTML escape
        authors.push(page.inbox_authors[i])
    }
    for (var i = 0; i < page.inbox_followers.length; i++) {
        // TODO: HTML escape
        authors.push(page.inbox_followers[i])
    }
    span.innerHTML = authors.join(",");
    div.appendChild(span);
    var span = document.createElement("span");
    span.className = "inboxtime";
    span.innerText = "18:13";
    div.appendChild(span);
    div.appendChild(document.createTextNode(page.text));
    var chapter = this;
    if (isnew) {
        div.addEventListener("click", function() {
            book.setPageUnread(page.pageBlobRef);
            document.getElementById("inbox").style.display = "none";
            document.getElementById("pagecontainer").style.display = "block"; 
            chapter.setActivePage(page);
        });
        if (page.inbox_selected) {
            $(div).addClass("inboxitemselected");
            input.checked = true;
        }
    }
    return div;
};

Chapter.prototype.getSelectedPages = function() {
    if (this.id != "inbox") {
        if (this.currentPage) {
            return [this.currentPage];
        }
        return [];
    }
    var result = [];
    for (var i = 0; i < this.pages.length; i++) {
        var page = this.pages[i];
        if (page.inbox_selected) {
            result.push(page);
        }
    }
    return result;
};

Chapter.prototype.setUnreadInfo = function(unread) {
    for (var i = 0; i < this.pages.length; i++) {
        var page = this.pages[i];
        if (unread[page.pageBlobRef]) {
            page.setUnread(true);
            if (unread && this.id == "inbox") {
                this.redrawInboxItem(page);
            }
        } else {
            page.setUnread(false);
        }
    }
    this.updateUnreadPagesCount();
};

Chapter.prototype.setPageUnread = function(perma_blobref, unread) {
    var dirty = false
    for (var i = 0; i < this.pages.length; i++) {
        var page = this.pages[i];
        if (page.pageBlobRef == perma_blobref && page.unread != unread) {
            dirty = true
            // The current page cannot be marked unread
            if (this == this.book.currentChapter && page == this.currentPage && unread) {
                continue;
            }
            page.setUnread(unread);
            if (unread && this.id == "inbox") {
                this.redrawInboxItem(page);
            }
        }
    }
    if (dirty) {
        this.updateUnreadPagesCount();
    }
};

Chapter.prototype.updateUnreadPagesCount = function() {
    var old = this.unreadPagesCount;
    this.unreadPagesCount = 0;
    for( var i = 0; i < this.pages.length; i++ ) {
        var p = this.pages[i];
        if (p.unread) {
            this.unreadPagesCount++;
        }
    }
    if (!this.tab || old == this.unreadPagesCount) {
        return
    }
    this.showUnreadPagesCount()
};

Chapter.prototype.showUnreadPagesCount = function() {
    var span = this.tab.lastChild;
    if (!$(span).hasClass("pagesunread")) {
        span = null;
    }
    if (this.unreadPagesCount > 0) {
        if (!span) {
            span = document.createElement("span");
            span.className = "pagesunread";
            this.tab.appendChild(span);
        }
        span.innerText = this.unreadPagesCount.toString();
    } else {
        if (span) {
            this.tab.removeChild(span);
        }
    }
};

// =========================================================
// Page
// =========================================================

Page.prototype.open = function() {
    if (this.vtab) {
        $(this.vtab).removeClass("inactivevtab" + this.chapter.colorScheme.toString());
        $(this.vtab).addClass("activevtab");
    }
    this.applyLayout_();
    this.showContents();
    this.showFollowers();
    // If the page blobref is marked with "tmp-" then the page has just been created and there is no need to open it.
    if (this.pageBlobRef.substring(0,4) != "tmp-") {
        // If opened from the inbox, mark it as read immediately
        store.openPage(this, this.chapter.id == "inbox");
    }
};

Page.prototype.close = function() {
    if (this.vtab) {
        $(this.vtab).removeClass("activevtab");
        $(this.vtab).addClass("inactivevtab" + this.chapter.colorScheme.toString());
    }
    // Cleanup
    var pagediv = document.getElementById("pagecontent");
    var content = document.getElementsByClassName("content");
    for( ; content.length > 0; ) {
        pagediv.removeChild(content[0]);
    }
    // Cleanup
    var sharediv = document.getElementById("share");
    var friends = document.getElementsByClassName("friend");
    for( ; friends.length > 0; ) {
        sharediv.removeChild(friends[0]);
    }
    // Close the current page
    if (this.pageBlobRef && this.pageBlobRef.substr(0,4) != "tmp-") {
        store.closePage(this);
    }
};

Page.prototype.renderTab = function() {
    this.vtab = document.createElement("div");
    if (this == this.chapter.currentPage) {
        this.vtab.className = "vtab activevtab";
    } else {
        this.vtab.className = "vtab inactivevtab" + this.chapter.colorScheme.toString();
    }
    this.vtab.appendChild(document.createTextNode(this.text));
    var p = this;
    this.vtab.addEventListener("click", function() {
        store.waitForPageIO( function() {
            p.chapter.setActivePage(p);
        });
    });
    if (this.unread) {
        var span = document.createElement("span");
        span.className = "authorsunread";
        span.innerHTML = "&nbsp";
        this.vtab.appendChild(span);
    }
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
    var pagediv = document.getElementById("pagecontent");
    var div = document.createElement("div");
    div.className = "content" + (content.cssClass ? " " + content.cssClass : "");
    div.appendChild(document.createTextNode(content.text));
    div.contentEditable = true;
    pagediv.appendChild(div);
/*
    if (content.layout == "title") {
        var pagediv = document.getElementById("pagecontent");
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
        var pagediv = document.getElementById("pagecontent");
        div = document.createElement("div");
        div.className = "content textbox";
        div.contentEditable = true;
        div.appendChild(document.createTextNode(content.text));
        pagediv.appendChild(div);
    } else {
        console.log("UNKNOWN layout")
        return;
    } */
    var editor = new LW.Editor(content, "text", div);
};

Page.prototype.addFollower = function(follower) {
    this.followers.push(follower);
    // Remove from invitations
    for( var i = 0; i < this.invitations.length; i++ ) {
        if (this.invitations[i].id == follower.id) {
            this.invitations.splice(i, 1);
            if (this.isVisible()) {
                var div = document.getElementById("invitee-" + follower.id);
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
    // HACK
    if (!inviteOnly) {
        div.id = "follower-" + follower.id;
        div.className = "friend friendonline";
    } else {
        div.className = "friend friendaway";
        div.id = "invitee-" + follower.id;
    }
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

Page.prototype.setUnread = function(unread) {
    if (this.unread == unread) {
        return;
    }
    this.unread = unread;
    if (!this.unread && this.inbox_latestauthors) {
        this.inbox_authors = this.inbox_latestauthors.concat(this.inbox_authors);
        this.inbox_latestauthors = [];
    }
    if (!this.vtab) {
        return;
    }
    var span = this.vtab.lastChild;
    if (!$(span).hasClass("authorsunread")) {
        span = null;
    }
    if (this.unread) {
        if (!span) {
            span = document.createElement("span");
            span.className = "authorsunread";
            this.vtab.appendChild(span);
        }
        span.innerHTML = "&nbsp";
    } else {
        if (span) {
            this.vtab.removeChild(span);
        }
    }
};

Page.prototype.setLayout = function(layout) {
    this.layout = layout;
    this.applyLayout_();
};

Page.prototype.applyLayout_ = function() {
    var pagecontentdiv = document.getElementById("pagecontent");
    var pagediv = document.getElementById("page");
    var scale = 1;
    if (this.layout && this.layout.style && this.layout.style["width"]) {
        scale = (pagediv.offsetWidth - 220) / parseInt(this.layout.style["width"]);
    }
    pagecontentdiv.style["-webkit-transform"] = "scale(" + scale.toString() + ")";
    pagecontentdiv.style.width = ((pagediv.offsetWidth - 220) / scale).toString() + "px";
    if (this.layout && this.layout.style && this.layout.style["height"]) {
        pagecontentdiv.style.height = this.layout.style["height"];
    } else {
        delete pagecontentdiv.style.height;
    }
};

PageContent.prototype.mutate = function(mutation) {
    if (mutation.field == "text") {
        console.log(JSON.stringify(mutation.op));
        lightwave.ot.ExecuteOperation(this, mutation.op);
    } else {
        console.log("Err: Unknown mutation field: " + mutation.field)
    }
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
