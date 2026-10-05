package com.github.lechandonga.contentaddressedobjectstorejava.store.web;

import com.github.lechandonga.contentaddressedobjectstorejava.store.api.GarbageCollectionReport;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectStore;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectVersion;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.PutResult;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Minimal HTTP facade over {@link ObjectStore}.
 *
 * <ul>
 *   <li>{@code PUT /objects/{id}} – upload; header {@code If-Match: <version>}
       performs a conditional commit ({@code 0} = create only).</li>
 *   <li>{@code GET /objects/{id}} – latest version; {@code version} query/path
 *       variants select older committed versions.</li>
 *   <li>{@code GET /objects/{id}/versions} – version history.</li>
 *   <li>{@code POST /gc} – run unreferenced-data reclamation.</li>
 * </ul>
 */
@RestController
@RequestMapping("/objects")
public class ObjectStoreController {

    private final ObjectStore store;

    public ObjectStoreController(ObjectStore store) {
        this.store = store;
    }

    @PutMapping(value = "/{id:.+}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public PutResult put(@PathVariable("id") String id,
                         @RequestBody byte[] body,
                         @RequestHeader(value = "If-Match", required = false) Long ifMatch) {
        long expected = ifMatch == null ? -1 : ifMatch;
        return store.put(id, new ByteArrayInputStream(body), expected);
    }

    @GetMapping(value = "/{id:.+}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void get(@PathVariable("id") String id,
                    @RequestHeader(value = "X-Version", required = false) Long version,
                    HttpServletResponse response) throws IOException {
        long wanted = version == null ? -1 : version;
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        try (OutputStream out = new BufferedOutputStream(response.getOutputStream())) {
            store.get(id, wanted, out);
        }
    }

    @GetMapping("/{id:.+}/versions")
    public List<ObjectVersion> versions(@PathVariable("id") String id) {
        return store.listVersions(id);
    }

    @PostMapping("/gc/run")
    public GarbageCollectionReport runGc() {
        return store.collectGarbage();
    }
}
