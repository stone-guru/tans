package org.axesoft.tans.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.lang3.Range;
import org.apache.http.HttpClientConnection;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class TansApacheClient implements TansClient {
    private static class AcquireRequest {
        final String key;
        final long number;
        final boolean ignoreLeader;
        public int times = 0;

        public AcquireRequest(String key, long number, boolean ignoreLeader) {
            this.key = key;
            this.number = number;
            this.ignoreLeader = ignoreLeader;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AcquireRequest that = (AcquireRequest) o;

            if (number != that.number) {
                return false;
            }
            if (ignoreLeader != that.ignoreLeader) {
                return false;
            }
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + (int) (number ^ (number >>> 32));
            result = 31 * result + (ignoreLeader ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "AcquireRequest{" +
                    "key='" + key + '\'' +
                    ", n=" + number +
                    ", ignoreLeader=" + ignoreLeader +
                    '}';
        }
    }


    private String[] servers;
    private CloseableHttpClient client;
    private LoadingCache<AcquireRequest, HttpPut> requestCache;
    private String token;
    private AtomicInteger requestTimes;
    private AtomicDouble totalRequestDuration;

    public TansApacheClient(String[] servers, String token) {
        this.servers = Arrays.copyOf(servers, servers.length);
        this.token = token;

        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(5);
        cm.setDefaultMaxPerRoute(1);

        this.client = HttpClients.custom()
                .setConnectionManager(cm)
                .build();


        this.requestCache = CacheBuilder.newBuilder()
                .concurrencyLevel(30)
                .expireAfterAccess(Duration.ofSeconds(10))
                .maximumSize(10000)
                .build(new CacheLoader<AcquireRequest, HttpPut>() {
                    @Override
                    public HttpPut load(AcquireRequest req) throws Exception {
                        int i = (int) (Math.random() * 1000) % servers.length;
                        String url = String.format("http://%s/v1/keys/%s?number=%d", servers[i], req.key, req.number);
                        return buildRequest(url);
                    }
                });

        this.requestTimes = new AtomicInteger(0);
        this.totalRequestDuration = new AtomicDouble(0.0);
    }

    private HttpPut buildRequest(String url) {
        HttpPut put = new HttpPut(url);
        put.addHeader("keep-alive", "true");
        put.addHeader("X-tans-token", this.token);
        return put;
    }

    @Override
    public Range<Long> acquire(String key, int n, boolean ignoreLeader) {
        int i = 0;
        HttpPut httpRequest;
        AcquireRequest acquireRequest = new AcquireRequest(key, n, false);
        while (i < 3) {
            try {
                httpRequest = this.requestCache.get(acquireRequest);
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            long t0 = System.nanoTime();
            try (CloseableHttpResponse response = client.execute(httpRequest)) {

                double millis = (System.nanoTime() - t0) / 1e+6;
                this.requestTimes.incrementAndGet();
                this.totalRequestDuration.addAndGet(millis);

                //System.out.println("Duration is " + millis);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200) {
                    String conent = null;
                    try {
                        conent = EntityUtils.toString(response.getEntity());
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return parseResult(conent);
                }
                else if (isRedirectCode(response.getStatusLine().getStatusCode())) {
                    String url = removeRequestSequence(response.getFirstHeader("location").getValue());
                    this.requestCache.put(acquireRequest, buildRequest(url));
                }
                else if (statusCode == 409 || statusCode == 503) { //conflict || unavailabled
                    continue;
                }
                else {
                    throw new RuntimeException("code " + statusCode + " when " + httpRequest.getURI());
                }
            }
            catch (ClientProtocolException e) {
                throw new RuntimeException(e);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            i++;
        }

        throw new RuntimeException("Failed more than " + i + " times");
    }

    @Override
    public double durationMillisPerRequest() {
        int times = this.requestTimes.get();
        double duration = this.totalRequestDuration.get();
        return times == 0? 0.0 : duration/times;
    }

    private String removeRequestSequence(String url){
        int idx = url.indexOf("&srn=");
        if(idx > 0){
            return url.substring(0, idx);
        }
        return url;
    }

    private Range<Long> parseResult(String content) {
        JsonNode node = null;
        try {
            node = new ObjectMapper().readValue(content, JsonNode.class);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        JsonNode range = node.get("range");
        long low = range.get(0).asLong();
        long high = range.get(1).asLong();

        return Range.between(low, high);
    }

    @Override
    public void close() {
        try {
            this.client.close();
        }
        catch (IOException e) {
        }
    }

    public static boolean isRedirectCode(int code) {
        switch (code) {
            case 300:
            case 301:
            case 302:
            case 303:
            case 305:
            case 307:
                return true;
            default:
                return false;
        }
    }
}
