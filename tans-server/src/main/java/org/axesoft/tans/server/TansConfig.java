package org.axesoft.tans.server;

import com.google.common.collect.ImmutableMap;
import org.axesoft.jaxos.JaxosSettings;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TansConfig {
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private TansConfig template = new TansConfig();

        public TansConfig build(){
            checkNotNull(template.jaxosSettings);
            checkNotNull(template.peerHttpPorts);

            return new TansConfig(template);
        }

        public Builder setJaxosSettings(JaxosSettings jaxosSettings){
            template.jaxosSettings = jaxosSettings;
            return this;
        }

        public Builder setPeerHttpPorts(Map<Integer, Integer> peerHttpPorts){
            template.peerHttpPorts = peerHttpPorts;
            return this;
        }

        public Builder setRequestBatchSize(int requestBatchSize){
            template.requestBatchSize = requestBatchSize;
            return this;
        }

        public Builder setLogHome(String logHome){
            template.logHome = logHome;
            return this;
        }

        public Builder setNettyBossThreadNumber(int nettyBossThreadNumber){
            template.nettyBossThreadNumber = nettyBossThreadNumber;
            return this;
        }

        public Builder setNettyWorkerThreadNumber(int nettyWorkerThreadNumber){
            template.nettyWorkerThreadNumber = nettyWorkerThreadNumber;
            return this;
        }
    }

    private JaxosSettings jaxosSettings;
    private Map<Integer, Integer> peerHttpPorts;
    private int requestBatchSize;
    private String logHome;
    private int nettyWorkerThreadNumber = 5;
    private int nettyBossThreadNumber = 1;
    private String httpToken = "0BC9F3";

    private TansConfig(){

    }

    private TansConfig(TansConfig other){
        this.jaxosSettings = other.jaxosSettings;
        this.peerHttpPorts = other.peerHttpPorts;
        this.requestBatchSize = other.requestBatchSize;
        this.logHome = other.logHome;
        this.nettyBossThreadNumber = other.nettyBossThreadNumber;
        this.nettyWorkerThreadNumber = other.nettyWorkerThreadNumber;
    }

    public int serverId(){
        return jaxosSettings.serverId();
    }

    public String address(){
        return jaxosSettings.self().address();
    }

    public int httpPort(){
        return getPeerHttpPort(jaxosSettings.serverId());
    }

    public JaxosSettings jaxConfig(){
        return this.jaxosSettings;
    }

    public int getPeerHttpPort(int peerId){
        return peerHttpPorts.getOrDefault(peerId, 0);
    }

    public int requestBatchSize() {
        return requestBatchSize;
    }

    public String logHome(){
        return this.logHome;
    }


    public int nettyWorkerThreadNumber() {
        return nettyWorkerThreadNumber;
    }

    public int nettyBossThreadNumber() {
        return nettyBossThreadNumber;
    }

    public String httpToken() {
        return httpToken;
    }

    @Override
    public String toString() {
        return "TansConfig{" +
                "jaxosSettings=" + jaxosSettings +
                ", peerHttpPorts=" + peerHttpPorts +
                ", requestBatchSize=" + requestBatchSize +
                ", logHome='" + logHome + '\'' +
                ", nettyWorkerThreadNumber=" + nettyWorkerThreadNumber +
                ", nettyBossThreadNumber=" + nettyBossThreadNumber +
                '}';
    }
}
