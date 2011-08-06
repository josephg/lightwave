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
    store.bookPermaInfo = null;
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
        var page = new Page(book.inbox, "page-" + jmsg.perma, jmsg.digest, null);
        page.digestAuthors = jmsg.authors;
        page.pageBlobRef = jmsg.perma;
        book.inbox.addPage(page);
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
        if (pi)  pi.dequeueOut();
    };
    if (pi) pi.inflight = true;
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
        console.log("Closed");
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
        // Install event handlers
        store.bookPermaInfo = store.get(perma);
        store.bookPermaInfo.onEntity = store.onBookEntity;
        // Replay the received blobs
        for( var i = 0; i < response.blobs.length; i++ ) {
            store.addOTNode(response.blobs[i]);
        }
    };
    store.httpPost("/private/open", JSON.stringify({perma:perma, session:store.sessionID}), f);    
};

store.openPage = function(page) {
    var f = function(msg) {
        console.log("Got: " + msg)
        var response = JSON.parse(msg);
        if ( !response.ok ) {
            alert(response.error);
            return;
        };
        console.log("Opened page: " + page.pageBlobRef);
        // Install event handlers
        var pagePermaInfo = store.get(page.pageBlobRef);
        pagePermaInfo.onEntity = store.onPageEntity;
        pagePermaInfo.onKeep = store.onPageKeep;
        pagePermaInfo.onPermission = store.onPagePermission;
        pagePermaInfo.onMutation = store.onPageMutation;
        // Replay the received blobs
        for( var i = 0; i < response.blobs.length; i++ ) {
            store.addOTNode(response.blobs[i]);
        }
    };
    var pi = store.get(page.pageBlobRef);
    store.httpPost("/private/open", JSON.stringify({perma:page.pageBlobRef, session:store.sessionID, from: pi.seq}), f);    
};

store.createChapterEntity = function(chapter) {
    var f = function(response) {
        chapter.id = response.blobref;
    };
    var content = {title: chapter.text, color: chapter.colorScheme};
    if (chapter.after && chapter.after != "inbox") {
        content.after = chapter.after;
    }
    var msg = {perma: store.bookPermaInfo.blobref, content: content, mimetype:"application/x-lightwave-entity-chapter", type: "entity"};
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
    var msg = {perma: store.bookPermaInfo.blobref, content: content, mimetype:"application/x-lightwave-entity-page", type: "entity"};
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
    if (permaInfo != store.bookPermaInfo) {
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
    // This book is currently not open? -> do nothing
    if ( !book || !book.currentChapter || !book.currentChapter.currentPage) {
        return
    }
    var page = book.currentChapter.currentPage
    if (permaInfo.blobref != page.pageBlobRef) {
        return
    }
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
        var after;
        for( var i = 0; i < response.items.length; i++ ) {
            var item = response.items[i];
            var page = new Page(book.inbox, "page-" + item.perma, item.digest, after);
            page.pageBlobRef = item.perma;
            page.digestAuthors = item.authors;
            book.inbox.addPage(page, false);
            after = page.id;
        }
    };
    store.httpGet("/private/listinbox", f);
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

// ------------------------------------------------
// PermaInfo

function PermaInfo(blobref) {
    this.blobref = blobref;
    this.seq = 0;
    this.queue = { };
    this.outqueue = [];
    this.inflight = false;
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
    this.inflight = false;
    if (this.outqueue.length == 0) {
        return;
    }
    var msg = this.outqueue.shift();
    console.log("DEQUEUE " + JSON.stringify(msg.blob));
    store.submit(msg.blob, msg.onsuccess, msg.onerror, msg.beforesend);
};

PermaInfo.prototype.addMutation = function(mut) {
    this.addOTNode_(mut);
    if (this.onMutation) {
        this.onMutation(this, mut);
    }
    this.dequeue_();
};

PermaInfo.prototype.addEntity = function(entity) {
    this.addOTNode_(entity);
    if (this.onEntity) {
        this.onEntity(this, entity);
    }
    this.dequeue_();
};

PermaInfo.prototype.addKeep = function(keep) {
    this.addOTNode_(keep);
    if (this.onKeep) {
        this.onKeep(this, keep);
    }
    this.dequeue_();
};

PermaInfo.prototype.addPermission = function(perm) {
    this.addOTNode_(perm);
    if (this.onPermission) {
        this.onPermission(this, perm);
    }
    this.dequeue_();
};

PermaInfo.prototype.addOTNode_ = function(node) {
    if (node.seq < this.seq) {
        console.log("Received ot node twice")
        return;
    }
    if (node.seq > this.seq) {
        console.log("Queueing up ot node")
        this.queue[node.seq] = node;
        return;
    }
    this.seq++;
    console.log("Apply seq " + (this.seq - 1).toString() + " of " + node.perma);
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
