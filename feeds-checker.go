package main

import (
    "compress/gzip"
    "encoding/xml"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "regexp"
    "time"
)

type FeedInfo struct {
    Stat           string
    Size           string
    VacanciesCount int64
    FailureSince   time.Time
}

const FeedsLimit = 32

func getFeedSize(url string) (size string, stat []byte) {
    statUrl := fmt.Sprintf("%s?stat", url)
    res, err := http.Get(statUrl)
    if err != nil {
        log.Printf("Error fetching stat from %s: %v\n", statUrl, err)
        return
    }
    defer res.Body.Close()
    if res.StatusCode >= 300 {
        log.Printf("Got '%v' from %s\n", res.Status, statUrl)
        return
    }
    stat, err = ioutil.ReadAll(res.Body)
    if err != nil {
        log.Printf("Error fetching stat from %s: %v\n", statUrl, err)
        return
    }

    re := regexp.MustCompile(`size:(\d+) bytes`)
    return string(re.FindSubmatch(stat)[1][:]), stat
}

func feedIsAlive(url string) bool {
    size, _ := getFeedSize(url)
    return size != ""
}

func updateInfoIfNeed(url string, feeds map[string]FeedInfo) error {
    size, stat := getFeedSize(url)
    if size == "" {
        return fmt.Errorf("Error getting feed %s size - skip info update\n", url)
    }

    fi, ok := feeds[url]
    if !ok || fi.Size != size {
        log.Printf("counting vacancies for %s", url)
        vc, err := countVacancies(url)
        if err != nil {
            return fmt.Errorf("Error counting vacancies: %v", err)
        }
        feeds[url] = FeedInfo{
            Stat:           string(stat[:]),
            Size:           size,
            VacanciesCount: vc,
        }
        log.Println(feeds[url].VacanciesCount)
    }
    return nil
}

func countVacancies(url string) (int64, error) {
    res, err := http.Get(url)
    if err != nil {
        return 0, fmt.Errorf("Error fetching archive from %s: %v", url, err)
    }
    defer res.Body.Close()

    uncompressedStream, err := gzip.NewReader(res.Body)
    if err != nil {
        return 0, fmt.Errorf("Error uncompressing response from %s: %v", url, err)
    }
    decoder := xml.NewDecoder(uncompressedStream)
    var count int64
    for {
        t, _ := decoder.Token()
        if t == nil {
            break
        }
        switch se := t.(type) {
        case xml.StartElement:
            if se.Name.Local == "vacancy" {
                count++
            }
        }
    }
    return count, nil
}

var info = make(map[string]FeedInfo, FeedsLimit)
var updaters = make(map[string]time.Time, FeedsLimit)

func feedInfoHandler(w http.ResponseWriter, r *http.Request) {
    values := r.URL.Query()
    urls, ok := values["url"]
    if !ok {
        w.WriteHeader(http.StatusBadRequest)
        return
    }
    url := urls[0]
    if len(url) == 0 {
        log.Printf("Error getting feed %s size - refuse serving\n", url)
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    _, ok = updaters[url]
    if !ok {

        if !feedIsAlive(url) {
            log.Printf("%s isn't alive - return 404", url)
            w.WriteHeader(http.StatusNotFound)
            return
        }

        if len(updaters) >= FeedsLimit {
            w.WriteHeader(http.StatusPaymentRequired)
            w.Write([]byte(fmt.Sprintf("Feeds limit (%d) is exhausted:\n", FeedsLimit)))
            for url, _ := range updaters {
                w.Write([]byte(fmt.Sprintf("%s\n", url)))
            }
            return
        }
        go func(c <-chan time.Time, url string) {
            for ; ; <-c {
                err := updateInfoIfNeed(url, info)
                if err != nil {
                    log.Println(err)
                    feed, ok := info[url]
                    if ok && feed.FailureSince.IsZero() {
                        feed.FailureSince = time.Now()
                    }
                    continue
                }

                if time.Since(updaters[url]) > (6 * time.Hour) {
                    log.Printf("info about %s is not requested for 6 hours - cancel monitoring", url)
                    delete(updaters, url)
                    delete(info, url)
                    return
                }
            }
        }(time.Tick(time.Minute), url)
    }
    updaters[url] = time.Now()

    feed, ok := info[url]
    if !ok {
        w.WriteHeader(http.StatusAccepted)
        return
    }
    if !feed.FailureSince.IsZero() && time.Since(feed.FailureSince) > 6*time.Hour {
        log.Printf("info about %s could not be updated for more than 6 hours - return error", url)
        w.WriteHeader(http.StatusExpectationFailed)
        w.Write([]byte("information could not be obtained for more than 6 hours"))
    }
    w.Write([]byte(fmt.Sprintf("%s, vacanciesCount: %v", feed.Stat, feed.VacanciesCount)))
}

func main() {
    http.HandleFunc("/feedinfo", feedInfoHandler)
    hostPort := ":8080"
    log.Printf("Listening on %s\n", hostPort)
    http.ListenAndServe(hostPort, nil)

    // url := "http://hh.ru/yandexvacancies.mvc.gz"
}
