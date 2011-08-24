if (!window.LW) {
    window.LW = {};
}

var seltarget;
var movx, movy;
var movtarget;
// The PageContent object that is being resized
var movcontent;
var resizetarget;
var resizex, resizey, resizew, resizeh, resized;
var resizexoff, resizeyoff;
var resizehandle;
var resizeang;
// The final degree
var resizealpha;
// The final width
var resizewidth;
// The final height
var resizeheight;
// The PageContent object that is being resized
var resizecontent;

LW.registerDnD = function() {
    document.getElementById("body").addEventListener("drop", function(e) {
        if (!book || !book.currentChapter || !book.currentChapter.currentPage) {
            return;
        }
        var page = book.currentChapter.currentPage;
        
        e.stopPropagation(); // Stops some browsers from redirecting.
        e.preventDefault();

        if (e.dataTransfer.files.length == 0) {
            var url = e.dataTransfer.getData('url');
            var img = new PageContent(page, "tmp-img-" + Math.random().toString(), url, "image", {});
            page.addContent(img);
            store.createContentEntity(img);
            return;
        }

        for( var i = 0; i < e.dataTransfer.files.length; i++ ) {
            var reader = new FileReader();
            reader.onload = function(evt) {
                var img = new PageContent(page, "tmp-img-" + Math.random().toString(), evt.target.result, "image", {});
                page.addContent(img);
                store.createContentEntity(img);
            };
            reader.readAsDataURL(e.dataTransfer.files[i]);
        }
    }, false);
    
    document.getElementById("body").addEventListener("dragover", function(e) {
        e.stopPropagation();
        e.preventDefault();
    }, false);

    $("body").bind({
        "mousedown": function(e) {
            if (seltarget) {
                $(seltarget).removeClass("selected");
            }
        },
        "mousemove": function(e) {
            if (movtarget) {
                e.preventDefault();
                e.stopPropagation();
                movcontent.move(movtarget.offsetLeft + e.clientX - movx, movtarget.offsetTop + e.clientY - movy);
//                movtarget.style.left = (movtarget.offsetLeft + e.clientX - movx).toString() + "px";
//                movtarget.style.top = (movtarget.offsetTop + e.clientY - movy).toString() + "px";
                movx = e.clientX;
                movy = e.clientY;
            } else if (resizetarget) {
                e.preventDefault();
                e.stopPropagation();
                var targ = e.target;
                var rx = e.clientX - resizex;
                var ry = e.clientY - resizey;
                var r = Math.sqrt(rx * rx + ry * ry);
                var d = 2 * (r + Math.sqrt(resizexoff * resizexoff + resizeyoff * resizeyoff));
                var w = resizew / resized * d;
                var h = resizeh / resized * d;
                var addw = parseInt(window.getComputedStyle(resizetarget, null).borderLeftWidth);
                addw += parseInt(window.getComputedStyle(resizetarget, null).borderRightWidth);
                addw += parseInt(window.getComputedStyle(resizetarget, null).paddingLeft);
                addw += parseInt(window.getComputedStyle(resizetarget, null).paddingRight);
                addw += parseInt(window.getComputedStyle(resizetarget, null).marginLeft);
                addw += parseInt(window.getComputedStyle(resizetarget, null).marginRight);
                var addh = parseInt(window.getComputedStyle(resizetarget, null).borderTopWidth);
                addh += parseInt(window.getComputedStyle(resizetarget, null).borderBottomWidth);
                addh += parseInt(window.getComputedStyle(resizetarget, null).paddingTop);
                addh += parseInt(window.getComputedStyle(resizetarget, null).paddingBottom);
                addh += parseInt(window.getComputedStyle(resizetarget, null).marginTop);
                addh += parseInt(window.getComputedStyle(resizetarget, null).marginBottom);
                resizecontent.move(resizeleft - (w - resizew)/2, resizetop - (h - resizeh)/2);
//                resizetarget.style.left = (resizeleft - (w - resizew)/2).toString() + "px";
//                resizetarget.style.top = (resizetop - (h - resizeh)/2).toString() + "px";
                //resizetarget.style.width = (w - addw).toString() + "px";
                //resizetarget.style.height = (h - addh).toString() + "px";
                resizecontent.resize(w - addw, h - addh);
                resizewidth = w - addw;
                resizeheight = h - addh;
//                resizetarget.firstChild.style.width = (w - addw).toString() + "px";
//                resizetarget.firstChild.style.height = (h - addh).toString() + "px";
                resizealpha = Math.asin(ry / r) / Math.PI * 180;
                if (rx < 0) resizealpha = 180 - resizealpha;
                if ($(resizehandle).hasClass("resize-se")) {
                    resizealpha -= resizeang;
                } else if ($(resizehandle).hasClass("resize-ne")) {
                    resizealpha += resizeang;
                } else if ($(resizehandle).hasClass("resize-sw")) {
                    resizealpha -= 180 - resizeang;
                } else if ($(resizehandle).hasClass("resize-nw")) {
                    resizealpha += 180 - resizeang;
                }
                if (resizealpha < 0) resizealpha = 360 + resizealpha; else resizealpha = resizealpha % 360;
                resizecontent.rotate(resizealpha);
//                resizetarget.style["-webkit-transform"] = "rotate(" + resizealpha.toString() + "deg)";
            }
        },
        "mouseleave" : function(e) {
            if (!movtarget && !resizetarget) {
                return;
            }
            if (movtarget) {
                var x = parseInt(window.getComputedStyle(movtarget, null).left);
                var y = parseInt(window.getComputedStyle(movtarget, null).top);
                movcontent.submitMove(x, y);
                movtarget = null;
            } else if (resizetarget && typeof resizewidth != "undefined") {
                var x = parseInt(window.getComputedStyle(resizetarget, null).left);
                var y = parseInt(window.getComputedStyle(resizetarget, null).top);
                resizecontent.submitRotate(x, y, resizewidth, resizeheight, resizealpha);
                resizetarget = null;
            }
        },
        "mouseup": function(e) {
            if (!movtarget && !resizetarget) {
                return;
            }
            e.preventDefault();
            e.stopPropagation();
            if (movtarget) {
                var x = parseInt(window.getComputedStyle(movtarget, null).left);
                var y = parseInt(window.getComputedStyle(movtarget, null).top);
                movcontent.submitMove(x, y);
                movtarget = null;
            } else if (resizetarget && typeof resizewidth != "undefined") {
                var x = parseInt(window.getComputedStyle(resizetarget, null).left);
                var y = parseInt(window.getComputedStyle(resizetarget, null).top);
                resizecontent.submitRotate(x, y, resizewidth, resizeheight, resizealpha);
                resizetarget = null;
            }
        }
    });
};

LW.movableMouseDown = function(e, content) {
    console.log("Move");
    e.preventDefault();
    e.stopPropagation();
    movcontent = content;
    var targ = e.target;
    while (!$(targ).hasClass("movable")) {
        targ = targ.parentNode;
    }
    if ($(targ).hasClass("movable") && !$(targ).hasClass("selected")) {
        if (seltarget) {
            $(seltarget).removeClass("selected");
        }
        $(targ).addClass("selected");
        seltarget = targ;
        return;
    }
    if ($(targ).hasClass("resize")) targ = targ.parentNode;
    movx = e.clientX;
    movy = e.clientY;
    movtarget = targ
};

LW.resizeMouseDown = function(e, content, undefined) {
    console.log("Resize");
    e.preventDefault();
    e.stopPropagation();
    resizecontent = content;
    resizewidth = undefined;
    resizeheight = undefined;
    resizealpha = undefined;
    var targ = e.target.parentNode;
    var pos = findPos(targ);
    resizetop = targ.offsetTop;
    resizeleft = targ.offsetLeft;
    resizex = pos[0] + targ.offsetWidth / 2;
    resizey = pos[1] + targ.offsetHeight / 2;
    resizew = targ.offsetWidth;
    resizeh = targ.offsetHeight;
    resized = Math.sqrt((resizeh * resizeh) + (resizew * resizew));
    resizeang = Math.asin(resizeh / resized) / Math.PI * 180;
    if ($(e.target).hasClass("resize-se")) {
        resizexoff = e.target.offsetWidth - e.offsetX;
        resizeyoff = e.target.offsetHeight - e.offsetY;
    } else if ($(e.target).hasClass("resize-ne")) {
        resizexoff = e.target.offsetWidth - e.offsetX;
        resizeyoff = e.offsetY;
    } else if ($(e.target).hasClass("resize-nw")) {
        resizexoff = e.offsetX;
        resizeyoff = e.offsetY;
    } else if ($(e.target).hasClass("resize-sw")) {
        resizexoff = e.offsetX;
        resizeyoff = e.target.offsetHeight - e.offsetY;
    }
    resizetarget = targ;
    resizehandle = e.target;
};

