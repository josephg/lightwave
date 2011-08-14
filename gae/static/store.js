var store = { };

store.init = function(userid, sessionid, token) {
    store.userID = userid;
    store.sessionID = sessionid;
    store.token = token;
    store.channel = new goog.appengine.Channel(token);
    store.socket = store.channel.open();
    store.socket.onopen = onOpened;
    store.socket.onmessage = onMessage;
    store.socket.onerror = onError;
    store.socket.onclose = onClose;
    store.openPermas = { };
};

function onOpened() {
    console.log("Opened channel")
};

function onClose() {
    console.log("Closed channel")
};

function onError() {
    console.log("Channel error")
};

function onMessage(msg) {
    console.log("Message: " + msg.data);

    var jmsg = JSON.parse(msg.data);
    store.addOTNode(jmsg);
};

store.get = function(blobref) {
    var x = store.openPermas[blobref];
    if ( x ) {
        return x;
    }
    x = new PermaInfo(blobref);
    store.openPermas[blobref] = x;
    return x;
};

store.addOTNode = function(jmsg) {
    if (jmsg.type == "mutation") {
        store.get(jmsg.perma).addMutation(jmsg);
    } else if (jmsg.type == "entity") {
        store.get(jmsg.perma).addEntity(jmsg);
    } else if (jmsg.type == "keep") {
        store.get(jmsg.perma).addKeep(jmsg);
    } else if (jmsg.type == "permission") {
        store.get(jmsg.perma).addPermission(jmsg);
    } else if (jmsg.type == "invitation") {
        console.log("INVITATION " + JSON.stringify(jmsg));
        var page = new Page(book.inbox, "page-" + jmsg.perma, jmsg.digest, null);
        page.inbox_authors = jmsg.authors;
        page.inbox_latestauthors = jmsg.latestauthors;
        page.inbox_followers = jmsg.followers;
        page.unread = true;
        page.pageBlobRef = jmsg.perma;
        book.inbox.addPage(page);
    } else if (jmsg.type == "notification") {
        // Is this in the inbox?
        if (book.inbox.getPageByPageBlobRef(jmsg.perma)) {
            store.getInboxItem(jmsg.perma);
        }
        var pi = store.get(jmsg.perma);
        // Is this really newer than the latest update we have seen?
        if (pi.sequenceNumber() <= jmsg.lastseq) {
            book.setPageUnread(jmsg.perma, true);
        }
    }
};

store.submit = function(blob, onsuccess, onerror, beforesend) {
    var pi;
    if (blob.perma) {
        pi = store.get(blob.perma);
        if (pi.inflight || store.paused) {
            pi.enqueueOut({blob:blob, onsuccess:onsuccess, onerror: onerror, beforesend: beforesend});
            return;
        }
    }
    var f = function(msg) {
        console.log("Got: " + msg)
        var response = JSON.parse(msg);
        if ( response.error ) {
            if (onerror) {
                onerror(response);
            } else {
                alert(response.error);
            }
            return;
        }
        if (blob.type == "permanode" && pi) {
            // Rewrite the permanode blobref
            delete store.openPermas[pi.blobref];
            pi.blobref = response.blobref;
            store.openPermas[pi.blobref] = pi;
            // Rewrite the permanode blobref for all enqueued messages as well
            for( var i = 0; i < pi.outqueue.length; i++ ) {
                pi.outqueue[i].blob.perma = pi.blobref;
            }
        }
        if (pi && response.seq) { pi.seq = response.seq + 1; }
        if (onsuccess) onsuccess(response);
        if (pi) pi.dequeueOut();
    };
    if (pi) pi.inflight = blob;
    if (beforesend) {
        beforesend(blob);
    }
    if (blob.type == "permanode") {
        // Remove temporary data
        delete blob.perma;
    } else if (blob.type == "mutation") {
        blob.at = pi.seq;
    }
    store.httpPost("/private/submit?session=" + store.sessionID, JSON.stringify(blob), f);
};

store.close = function(perma) {
    var f = function(msg) {
        console.log("Got: " + msg)
        var response = JSON.parse(msg);
        if ( !response.ok ) {
            alert(response.error);
            return;
        };
        console.log("Closed " + perma);
    };
    store.httpPost("/private/close", JSON.stringify({perma:perma, session:store.sessionID}), f);
};

store.inviteByMail = function(follower, mail) {
    store.httpPost("/private/invitebymail", JSON.stringify({"user": follower.id, "content": mail}));    
};

store.invite = function(follower, onsuccess) {
    var f = function(response) {
        console.log("Invited a user");
        if (onsuccess) {
            onsuccess(response.knownuser);
        }
    }
    var page = follower.page;
    var pi = store.get(page.pageBlobRef);
    var msg = {type: "permission", perma: pi.blobref, allow: 1+2, deny:0, action:"invite", user: follower.id};
    store.submit(msg, f, null, null);   
};

store.loadBook = function() {
    var f = function(msg) {
        console.log("Got: " + msg)
        var response = JSON.parse(msg);
        if ( !response.ok ) {
            alert(response.error);
            return;
        };
        if (response.permas.length == 0) {
            alert("No book on server");
            return;
        }
        // Client supports only one book. Fetch it
        store.openBook(response.permas[0]);
        console.log("Found the book");
    };
    store.httpGet("/private/listpermas?mimetype=application/x-lightwave-book", f);
};

store.openBook = function(perma) {
    var f = function(msg) {
        console.log("Got: " + msg);
        var response = JSON.parse(msg);
        if ( !response.ok ) {
            alert(response.error);
            return;
        };
        console.log("Opened book");
        book.id = perma;
        // Replay the received blobs
        for( var i = 0; i < response.blobs.length; i++ ) {
            store.addOTNode(response.blobs[i]);
        }
        store.loadUnread();
    };

    // Install event handlers
    var pi = store.get(perma);
    pi.onEntity = store.onBookEntity;

    store.httpPost("/private/open", JSON.stringify({perma:perma, session:store.sessionID}), f);    
};

store.openPage = function(page, markasread) {
    var f = function(msg) {
        console.log("Got: " + msg)
        var response = JSON.parse(msg);
        if ( !response.ok ) {
            alert(response.error);
            return;
        };
        console.log("Opened page: " + page.pageBlobRef);
        var haskeep = false;
        var permission;
        // Replay the received blobs
        for( var i = 0; i < response.blobs.length; i++ ) {
            var blob = response.blobs[i];
            if (blob.type == "keep" && blob.signer == store.userID) {
                haskeep = true;
            } else if (!permission && blob.type == "permission" && blob.user == store.userID) {
                permission = blob.blobref;
            }
            store.addOTNode(blob);
        }
        // The user wants to see this page. This is the right time to issue a keep (if this did not happen before).
        // This means the user is now following the file.
        if (!page.getFollower(store.userID) && !haskeep) {
            console.log("NO KEEP YET. Sending one");
            // Send out a keep
            store.submit({type:"keep", perma:page.pageBlobRef, permission:permission}, null, null, null);
            if (pi.onKeep) {
                pi.onKeep(store.get(page.pageBlobRef), {type:"keep", perma:page.pageBlobRef, permission:permission, "signer": store.userID});
            }
        }
    };
    var pi = store.get(page.pageBlobRef);
    // Install event handlers
    pi.onEntity = store.onPageEntity;
    pi.onKeep = store.onPageKeep;
    pi.onPermission = store.onPagePermission;
    pi.onMutation = store.onPageMutation;
    pi.seq = page.nextSeq;
    store.httpPost("/private/open", JSON.stringify({perma:page.pageBlobRef, session:store.sessionID, from: pi.seq, markasread:markasread}), f);

    if (markasread) {
        book.setPageUnread(page.pageBlobRef, false);
    }
};

store.closePage = function(page) {
    var pi = store.get(page.pageBlobRef);
    page.nextSeq = pi.seq;
    store.close(page.pageBlobRef);
};

store.createChapterEntity = function(chapter) {
    var f = function(response) {
        chapter.id = response.blobref;
    };
    var content = {title: chapter.text, color: chapter.colorScheme};
    if (chapter.after && chapter.after != "inbox") {
        content.after = chapter.after;
    }
    var msg = {perma: book.id, content: content, mimetype:"application/x-lightwave-entity-chapter", type: "entity"};
    store.submit(msg, f, null);
};

store.createPageEntity = function(page) {
    var f = function(response) {
        page.id = response.blobref;
    };
    var content = {title: page.text, page: page.pageBlobRef, chapter: page.chapter.id};
    if (page.after) {
        content.after = page.after;
    }
    var msg = {perma: book.id, content: content, mimetype:"application/x-lightwave-entity-page", type: "entity"};
    store.submit(msg, f, null, function(m) { m.content.chapter = page.chapter.id; } );
};

store.createPage = function(page) {
    var f = function(response) {
        page.pageBlobRef = response.blobref;
        store.openPage(page);
        store.createPageEntity(page);
    };
    store.submit({type:"permanode", mimetype:"application/x-lightwave-page", perma:page.pageBlobRef}, f, null);
};

store.createContentEntity = function(pageContent) {
    var f = function(response) {
        console.log("REWRITING content " + pageContent.layout);
        pageContent.id = response.blobref;
    };
    var content = {layout: pageContent.layout, text: pageContent.text};
    var msg = {perma: pageContent.page.pageBlobRef, content: content, mimetype:"application/x-lightwave-entity-content", type: "entity"};
//    store.submit(msg, f, null, function(m) { m.perma = pageContent.page.pageBlobRef; console.log("REWRITING " + m.perma); } );
    store.submit(msg, f, null, null );
};

store.onBookEntity = function(permaInfo, entity) {
    // This book is currently not open? -> do nothing
    if (permaInfo.blobref != book.id) {
        return
    }
    if (entity.mimetype == "application/x-lightwave-entity-chapter") {
        var c = new Chapter(book, entity.blobref, entity.content.title, entity.content.color, entity.content.after ? entity.content.after : "inbox");
        book.addChapter(c, false);
    } else if (entity.mimetype == "application/x-lightwave-entity-page") {
        var c = book.getChapter(entity.content.chapter);
        if (!c) {
            console.log("Chapter not found. Ignoring page");
        }
        var p = new Page(c, entity.blobref, entity.content.title, entity.content.after);
        p.pageBlobRef = entity.content.page;
        c.addPage(p, false);
    }
};

store.onPageEntity = function(permaInfo, entity) {
    // This book is currently not open? -> do nothing
    if ( !book || !book.currentChapter || !book.currentChapter.currentPage) {
        return
    }
    var page = book.currentChapter.currentPage
    if (permaInfo.blobref != page.pageBlobRef) {
        return
    }
    if (entity.mimetype == "application/x-lightwave-entity-content") {
        var c = new PageContent(page, entity.blobref, entity.content.text, entity.content.layout);
        page.addContent(c);
    } else {
        console.log("Unknown entity type");
    }
};

store.onPageKeep = function(permaInfo, keep) {
    console.log("??????? onPageKeep for " + keep.signer);
    // This book is currently not open? -> do nothing
    if ( !book || !book.currentChapter || !book.currentChapter.currentPage) {
        return
    }
    var page = book.currentChapter.currentPage
    if (permaInfo.blobref != page.pageBlobRef) {
        return
    }
    // Follower is already known? Do nothing. 
    if (page.getFollower(keep.signer)) {
        return
    }
    var f = page.getInvitation(keep.signer);
    if (!f) {
        f = new Follower(page, keep.signer, null);
    }
    page.addFollower(f);
};

store.onPagePermission = function(permaInfo, perm) {
    // This book is currently not open? -> do nothing
    if ( !book || !book.currentChapter || !book.currentChapter.currentPage) {
        return
    }
    var page = book.currentChapter.currentPage
    if (permaInfo.blobref != page.pageBlobRef) {
        return
    }
    if (page.getInvitation(perm.user)) {
        return
    }
    var f = new Follower(page, perm.user, null);
    page.addInvitation(f);
};

store.onPageMutation = function(permaInfo, mutation) {
    // This book is currently not open? -> do nothing
    if ( !book || !book.currentChapter || !book.currentChapter.currentPage) {
        return
    }
    var page = book.currentChapter.currentPage
    if (permaInfo.blobref != page.pageBlobRef) {
        return
    }
    var content = page.getContent(mutation.entity);
    if (!content) {
        console.log("Unknown entity");
        return
    }
    content.mutate(mutation);
};

store.loadInbox = function() {
    var f = function(msg) {
        var response = JSON.parse(msg);
        if (!response.ok) {
            alert(response.error);
            return;
        }
        console.log("INBOX: " + JSON.stringify(response.items));
        var after;
        for( var i = 0; i < response.items.length; i++ ) {
            var item = response.items[i];
            var page = new Page(book.inbox, "page-" + item.perma, item.digest, after);
            page.pageBlobRef = item.perma;
            page.inbox_authors = item.authors;
            page.inbox_latestauthors = item.latestauthors;
            page.inbox_followers = item.followers;
            page.inbox_latestseq = item.latestseq;
            book.inbox.addPage(page, false);
            after = page.id;
        }
    };
    store.httpGet("/private/listinbox", f);
};

store.loadUnread = function() {
    var f = function(msg) {
        var response = JSON.parse(msg);
        if (!response.ok) {
            alert(response.error);
            return;
        }
        console.log("UNREAD: " + JSON.stringify(response.unread));
        book.setUnreadInfo(response.unread);
    }
    store.httpGet("/private/listunread", f);
};

store.markAsRead = function(perma, seq) {
    var page = book.inbox.getPageByPageBlobRef(perma);
    if (page) {
        page.inbox_authors = page.inbox_authors.concat(page.inbox_latestauthors);
        page.inbox_latestauthors = [];
        book.inbox.redrawInboxItem(page);
    }
    book.setPageUnread(perma, false);

    store.httpGet( "/private/markasread?perma=" + perma + "&seq=" + seq.toString(), null );
};

store.getInboxItem = function(perma) {
    var f = function(msg) {
        console.log("INBOX  ITEM: " + msg);
        var response = JSON.parse(msg);
        if (!response.ok) {
            alert(response.error);
            return;
        }
        var item = response.item;
        var page = book.inbox.getPageByPageBlobRef(item.perma);
        if (!page) {
            return;
        }
        page.inbox_authors = item.authors;
        page.inbox_latestauthors = item.latestauthors;
        page.inbox_followers = item.followers;
        page.inbox_latestseq = item.latestseq;
        page.text = item.digest;
        book.inbox.redrawInboxItem(page);
    };

    store.httpGet( "/private/inboxitem?perma=" + perma, f );
};

store.httpPost = function(url, data, f) {
    var xmlHttp = null;
    try {
        // Mozilla, Opera, Safari sowie Internet Explorer (ab v7)
        xmlHttp = new XMLHttpRequest();
    } catch(e) {
        try {
            // MS Internet Explorer (ab v6)
            xmlHttp  = new ActiveXObject("Microsoft.XMLHTTP");
        } catch(e) {
            try {
                // MS Internet Explorer (ab v5)
                xmlHttp  = new ActiveXObject("Msxml2.XMLHTTP");
            } catch(e) {
                xmlHttp  = null;
            }
        }
    }
    if (xmlHttp) {
        xmlHttp.open('POST', url, true);
        xmlHttp.onreadystatechange = function () {
            if (xmlHttp.readyState == 4) {
                if ( f ) {
                    f(xmlHttp.responseText)
                }
            }
        };
        xmlHttp.send(data);
    }
};

store.httpGet = function(url, f) {
    var xmlHttp = null;
    try {
        // Mozilla, Opera, Safari sowie Internet Explorer (ab v7)
        xmlHttp = new XMLHttpRequest();
    } catch(e) {
        try {
            // MS Internet Explorer (ab v6)
            xmlHttp  = new ActiveXObject("Microsoft.XMLHTTP");
        } catch(e) {
            try {
                // MS Internet Explorer (ab v5)
                xmlHttp  = new ActiveXObject("Msxml2.XMLHTTP");
            } catch(e) {
                xmlHttp  = null;
            }
        }
    }
    if (xmlHttp) {
        xmlHttp.open('GET', url, true);
        xmlHttp.onreadystatechange = function () {
            if (xmlHttp.readyState == 4) {
                if ( f ) {
                    f(xmlHttp.responseText)
                }
            }
        };
        xmlHttp.send();
    }
};

store.pauseQueues = function() {
    store.paused = true;
};

store.resumeQueues = function() {
    store.paused = false;
    for( var key in store.openPermas) {
        var pi = store.openPermas[key];
        if (pi.inflight) {
            continue;
        }
        pi.dequeueOut();
    }
};

// Waits until all outgoing messages for this page are sent and
// then executes f. All user interactions are block in the meantime.
store.waitForPageIO = function(f) {
    if (!book || !book.currentChapter || !book.currentChapter.currentPage) {
        f();
        return true;
    }
    var page = book.currentChapter.currentPage;
    var pi = store.get(page.pageBlobRef);
    if (!pi.inflight) {
        f();
        return true;
    }
    showWaitScreen();
    pi.onIdle = function() {
        hideWaitScreen();
        delete pi.onIdle;
        f();
    }
    return false;
};

// ------------------------------------------------
// PermaInfo

function PermaInfo(blobref) {
    this.blobref = blobref;
    this.seq = 0;
    this.queue = { };
    this.outqueue = [];
    this.inflight = null;
    this.onIdle = null;
};

PermaInfo.prototype.enqueueOut = function(msg) {
    console.log("ENQUEUE " + JSON.stringify(msg.blob));
    if ( msg.blob.type == "mutation" && this.outqueue.length > 0) {
        var last = this.outqueue[this.outqueue.length - 1];
        if (last.blob.type == "mutation" && last.blob.entity == msg.blob.entity) {
            var tmp = lightwave.ot.ComposeOperation(last.blob.op, msg.blob.op);
            if (!tmp[1]) { // If compress succeeds. It should always, just being defensive here
                last.blob.op = tmp[0];
                return;
            } else {
                console.log("ERR composing: " + tmp[1]);
            }
        }
    }
    this.outqueue.push(msg);
};

PermaInfo.prototype.dequeueOut = function() {
    this.inflight = null;
    if (this.outqueue.length == 0) {
        if (this.onIdle) {
            this.onIdle();
        }
        return;
    }
    var msg = this.outqueue.shift();
    console.log("DEQUEUE " + JSON.stringify(msg.blob));
    store.submit(msg.blob, msg.onsuccess, msg.onerror, msg.beforesend);
};

PermaInfo.prototype.transformIncoming = function(blob) {
    // TODO: transform permissions
    if (blob.type != "mutation" || !blob.op) {
        return;
    }
    // Find all queued mutations of the same entity
    var lst = [];
    if (this.inflight && this.inflight.type == "mutation" && this.inflight.entity == blob.entity && this.inflight.field == blob.field) {
        lst.push(this.inflight);
    }
    for( var i = 0; i < this.outqueue.length; i++) {
        var msg = this.outqueue[i];
        if (msg.blob.type == "mutation" && msg.blob.entity == blob.entity && msg.blob.field == blob.field) {
            lst.push(msg.blob);
        }
    }
    // Transform
    for( var i = 0; i < lst.length; i++) {
        var client = lst[i];
        var tmp = lightwave.ot.TransformOperation(blob.op, client.op);
        if (tmp[2]) {
            console.log("ERR transforming: " + tmp[2]);
            // In this case we should turn this into the empty transformation
            blob.op = null;
            return blob;
        }
        blob.op = tmp[0];
        client.op = tmp[1];
    }
};

PermaInfo.prototype.addMutation = function(mut) {
    if (!this.addOTNode_(mut)) {
        return
    }
    this.transformIncoming(mut);
    if (this.onMutation) {
        this.onMutation(this, mut);
    }
    this.dequeue_();
};

PermaInfo.prototype.addEntity = function(entity) {
    if (!this.addOTNode_(entity)) {
        return;
    }
    if (this.onEntity) {
        this.onEntity(this, entity);
    }
    this.dequeue_();
};

PermaInfo.prototype.addKeep = function(keep) {
    if (!this.addOTNode_(keep)) {
        return;
    }
    if (this.onKeep) {
        this.onKeep(this, keep);
    }
    this.dequeue_();
};

PermaInfo.prototype.addPermission = function(perm) {
    if (!this.addOTNode_(perm)) {
        return;
    }
    if (this.onPermission) {
        this.onPermission(this, perm);
    }
    this.dequeue_();
};

PermaInfo.prototype.addOTNode_ = function(node) {
    if (node.seq < this.seq) {
        console.log("Received ot node twice")
        return false;
    }
    if (node.seq > this.seq) {
        console.log("Queueing up ot node")
        this.queue[node.seq] = node;
        return false;
    }
    this.seq++;
    console.log("Apply seq " + (this.seq - 1).toString() + " of " + node.perma);
    return true
};

PermaInfo.prototype.dequeue_ = function() {
    var next = this.queue[this.seq];
    if (next) {
        delete this.queue[this.seq];
        PermaInfo.addOTNode(next);
    }
};

PermaInfo.prototype.sequenceNumber = function() {
    return this.seq;
};
