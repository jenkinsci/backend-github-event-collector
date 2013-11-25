package org.jenkinsci.backend.ghec

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

import java.util.logging.Level
import java.util.logging.Logger

public static final LOGGER = Logger.getLogger(App.class.name)

/**
 * Checks if there was a change in the events basd on the ETag value.
 */
class EventsChangeTracker {
    private static final OM = new ObjectMapper()

    def url = new URL("https://api.github.com/orgs/jenkinsci/events");
    private String etag;
    int pollInterval = 30;

    /**
     * @return true
     *      if the event should be re-retrieved.
     */
    void next() {
        HttpURLConnection con = url.openConnection();
        if (etag!=null)
            con.setRequestProperty("If-None-Match",etag)

        con.connect()
        etag = con.getHeaderField("ETag")
        def pi = con.getHeaderField("X-Poll-Interval")
        if (pi !=null)  pollInterval = pi as int

        if (con.responseCode==304)
            return;

        while (true) {// for pagenation
            def r = new InputStreamReader(con.inputStream, "UTF-8");
            ArrayNode t = OM.readTree(r)

            for (ObjectNode event : t) {
                def id = event.get("id").asText()
                def ym = event.get("created_at").asText().substring(0,7)

                File tmp = new File("${ym}/${id}.tmp")
                File dst = new File("${ym}/${id}.json")

                if (dst.exists())
                    return; // we know this ID and presumably all the earlier ones

                dst.parentFile.mkdirs()

                LOGGER.info("Storing ${id}")
                OM.writeValue(tmp,event)
                tmp.renameTo(dst)
            }

            def nextLink = findNextLink(con)
            if (nextLink==null) {
                LOGGER.warning("Went all the way. Possible event loss")
                return;
            }
            con = url.openConnection()
        }
    }

    private URL findNextLink(HttpURLConnection con) {
        def next = con.headerFields["Link"].find { it.contains("rel=\"next\"") }
        if (next==null) return null;

        return new URL(next.replaceAll(".*<(.*)>.*","\$1"))
    }
}

def tracker = new EventsChangeTracker();

while (true) {
    try {
        tracker.next()
    } catch (IOException e) {
        LOGGER.log(Level.WARNING,"I/O problem", e)
    }
    Thread.sleep(tracker.pollInterval*1000);
}