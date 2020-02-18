package org.axesoft.jaxos.algo;

import java.util.function.Function;

public class ProposeResult<T> {
    public enum Code {
        SUCCESS, REDIRECT, ERROR
    }

    public static <V> ProposeResult<V> success(V value){
        return new ProposeResult<>(value);
    }

    public static <V> ProposeResult<V> redirectTo(int serverId){
        return new ProposeResult<>(serverId);
    }

    public static <V> ProposeResult<V> fail(String msg){
        return new ProposeResult<>(msg);
    }

    public static <A,B> ProposeResult<B> cast(ProposeResult<A> a){
        if(a.isSuccess()){
            throw new IllegalArgumentException();
        }
        return (ProposeResult<B>)a;
    }

    private Code code;
    private int redirectServerId;
    private T value;
    private String errorMessage;


    private ProposeResult(T value) {
        this.code = Code.SUCCESS;
        this.value = value;
    }

    private ProposeResult(int serverId){
        this.code = Code.REDIRECT;
        this.redirectServerId = serverId;
    }

    private ProposeResult(String errorMessage){
        this.code = Code.ERROR;
        this.errorMessage = errorMessage;
    }

    public Code code() {
        return code;
    }

    public boolean isSuccess(){
        return this.code == Code.SUCCESS;
    }

    public boolean isRedirect(){
        return this.code == Code.REDIRECT;
    }

    public int redirectServerId() {
        return redirectServerId;
    }

    public T value() {
        return value;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public <B> ProposeResult<B> map(Function<T, B> f){
        if(isSuccess()){
            return new ProposeResult<>(f.apply(this.value));
        } else {
            return (ProposeResult<B>)this;
        }
    }
}
