package lightwaveimporter

import (
  "xml"
  "bytes"
//  "log"
  "os"
  "json"
  "strconv"
  "strings"
  grapher "lightwavegrapher"
)

var content string = `
<book title="Lecture">
 <chapters>
  <chapter title="Chapter 1" color="0">
   <pages>
    <page title="Intro">
     <entities>
      <content layout="title">Introduction</content>
      <content layout="textbox">
        * Organization
        * Cloud Computing
        * HTML5 applications
        * Mobile HTML5 applications
        * Native Mobile Apps
      </content>
     </entities>
    </page>
   </pages>
  </chapter>
 </chapters>
</book>`

/*
type Book struct {
  Title string `xml:"attr"`
  Chapters []Chapter `xml:"chapters>chapter"`
}

type Chapter struct {
  Title string `xml:"attr"`
  Pages []Page `xml:"pages>page"`
}

type Page struct {
  Title string `xml:"attr"`
  Entities []Content `xml:"entities>content"`
}

type Content struct {
  Layout string `xml:"attr"`
  Text string `xml:"chardata"`
}
*/

type Book struct {
  Title string "attr"
  Chapters []Chapter "chapters>chapter"
}

type Chapter struct {
  Title string "attr"
  Color string "attr"
  Pages []Page "pages>page"
}

type Page struct {
  Title string "attr"
  Entities []Content "entities>content"
  Style string "attr"
}

type Content struct {
  Id string "attr"
  CssClass string "attr"
  Text string "chardata"
  Style string "attr"
}

func Import(g *grapher.Grapher, book *Book) os.Error {
  bookPerma, err := g.CreatePermaBlob("application/x-lightwave-book")
  if err != nil {
    return err
  }
  _, err = g.CreateKeepBlob(bookPerma.BlobRef(), "")
  if err != nil {
    return err
  }

  for _, ch := range book.Chapters {
    ch.Title = trimSpace(ch.Title)
    color, err := strconv.Atoi(ch.Color)
    if err != nil {
      return err
    }    
    jscontent := map[string]interface{}{"title": ch.Title, "color": color}
    content, err := json.Marshal(jscontent)
    chapterEntity, err := g.CreateEntityBlob(bookPerma.BlobRef(), "application/x-lightwave-entity-chapter", content);
    if err != nil {
      return err
    }
    
    pageEntities := []grapher.AbstractNode{}
    for pindex, p := range ch.Pages {
      p.Title = trimSpace(p.Title)
      perma, err := g.CreatePermaBlob("application/x-lightwave-page")
      if err != nil {
	return err
      }
      _, err = g.CreateKeepBlob(perma.BlobRef(), "")
      if err != nil {
	return err
      }
      
      jscontent = map[string]interface{}{}
      styles := strings.Split(p.Style, ";", -1)
      style := map[string]interface{}{}
      for _, s := range styles {
	i := strings.Index(s, ":")
	if i != -1 {
	  style[s[0:i]] = s[i+1:]
	}
      }
      jscontent["style"] = style
      content, err = json.Marshal(jscontent)
      _, err = g.CreateEntityBlob(perma.BlobRef(), "application/x-lightwave-entity-page-layout", content);
      if err != nil {
	return err
      }
      
      jscontent = map[string]interface{}{"title": p.Title, "page": perma.BlobRef(), "chapter": chapterEntity.BlobRef()}
      if pindex > 0 {
	jscontent["after"] = pageEntities[pindex - 1].BlobRef();
      }
      content, err = json.Marshal(jscontent)
      pageEntity, err := g.CreateEntityBlob(bookPerma.BlobRef(), "application/x-lightwave-entity-page", content);
      if err != nil {
	return err
      }
      pageEntities = append(pageEntities, pageEntity)
      
      for _, e := range p.Entities {
	e.Text = trimSpace(e.Text)
	jscontent := map[string]interface{}{"text": e.Text, "id": e.Id}
	if e.CssClass != "" {
	  jscontent["cssclass"] = e.CssClass
	}
	styles := strings.Split(e.Style, ";", -1)
	style := map[string]interface{}{}
	for _, s := range styles {
	  i := strings.Index(s, ":")
	  if i != -1 {
	    style[s[0:i]] = s[i+1:]
	  }
	}
	jscontent["style"] = style
	content, err := json.Marshal(jscontent)
	if err != nil {
	  return err
	}
	_, err = g.CreateEntityBlob(perma.BlobRef(), "application/x-lightwave-entity-content", content);
	if err != nil {
	  return err
	}      
      }
    }
  }
  return nil
}

func Parse(content string) (book *Book, err os.Error) {
  contentio := bytes.NewBufferString(content)
  var b Book
  err = xml.Unmarshal(contentio, &b)
  if err != nil {
    return nil, err
  }
  return &b, nil
}

func trimSpace(text string) string {
  lines := strings.Split(text, "\n", -1)
  for i, l := range lines {
    lines[i] = strings.TrimSpace(l)
  }
  return strings.Join(lines, "\n")
}

/*
func main() {
  contentio := bytes.NewBufferString(content)
  var b Book
  err := xml.Unmarshal(contentio, &b)
  if err != nil {
    println(err.String())
  }
  log.Printf("Result %v", b)
}
*/