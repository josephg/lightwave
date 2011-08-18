if (!window.LW) {
    window.LW = {};
}

 var seltarget;
 var movx, movy;
 var movtarget;
 var resizetarget;
 var resizex, resizey, resizew, resizeh, resized;
 var resizexoff, resizeyoff;
 var resizehandle;
 var resizeang;

LW.registerDnD = function() {
    document.getElementById("body").addEventListener("drop", function(e) {
        if (!book || !book.currentChapter || !book.currentChapter.currentPage) {
            return;
        }
        var page = book.currentChapter.currentPage;
        
        e.stopPropagation(); // Stops some browsers from redirecting.
        e.preventDefault();
        console.log(e);
        
        for( var i = 0; i < e.dataTransfer.files.length; i++ ) {
            var reader = new FileReader();
            reader.onload = function(evt) {
                var img = new PageContent(page, "tmp-img-" + Math.random().toString(), evt.target.result, "image", {});
                page.addContent(img);
                // TODO: store
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
                movtarget.style.left = (movtarget.offsetLeft + e.clientX - movx).toString() + "px";
                movtarget.style.top = (movtarget.offsetTop + e.clientY - movy).toString() + "px";
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
                resizetarget.style.left = (resizeleft - (w - resizew)/2).toString() + "px";
                resizetarget.style.top = (resizetop - (h - resizeh)/2).toString() + "px";
                //resizetarget.style.width = (w - addw).toString() + "px";
                //resizetarget.style.height = (h - addh).toString() + "px";
                resizetarget.firstChild.style.width = (w - addw).toString() + "px";
                resizetarget.firstChild.style.height = (h - addh).toString() + "px";
                var alpha = Math.asin(ry / r) / Math.PI * 180;
                if (rx < 0) alpha = 180 - alpha;
                if ($(resizehandle).hasClass("resize-se")) {
                    alpha -= resizeang;
                } else if ($(resizehandle).hasClass("resize-ne")) {
                    alpha += resizeang;
                } else if ($(resizehandle).hasClass("resize-sw")) {
                    alpha -= 180 - resizeang;
                } else if ($(resizehandle).hasClass("resize-nw")) {
                    alpha += 180 - resizeang;
                }
                if (alpha < 0) alpha = 360 + alpha; else alpha = alpha % 360;
                resizetarget.style["-webkit-transform"] = "rotate(" + alpha.toString() + "deg)";
            }
        },
        "mouseleave" : function(e) {
            movtarget = null;
            resizetarget = null;
        },
        "mouseup": function(e) {
            if (movtarget || resizetarget) {
                movtarget = null;
                resizetarget = null;
                e.preventDefault();
                e.stopPropagation();
            }
        }
    });
};

LW.movableMouseDown = function(e) {
    e.preventDefault();
    e.stopPropagation();
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
        console.log("w=" + seltarget.offsetWidth.toString());
        return;
    }
    if ($(targ).hasClass("resize")) targ = targ.parentNode;
    movx = e.clientX;
    movy = e.clientY;
    movtarget = targ
};

LW.resizeMouseDown = function(e) {
    e.preventDefault();
    e.stopPropagation();
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

