package org.axesoft.jaxos.base;


import java.util.Optional;

public abstract class Either<A, B> {

    public static <L, R> Either<L, R> right(R value) {
        if(value == null){
            throw new NullPointerException("Either.right should not be null");
        }
        return new Right<L, R>(value);
    }

    public static <L, R> Either<L, R> left(L value) {
        if(value == null){
            throw new NullPointerException("Either.left should not be null");
        }

        return new Left<L, R>(value);
    }

    public static <L, R> Either<L, R> fromNullable(L leftValue, R rightValue) {
        if (rightValue == null) {
            return Either.left(leftValue);
        } else {
            return Either.right(rightValue);
        }
    }


    public abstract boolean isRight();

    public abstract A getLeft();

    public abstract B getRight();

    public abstract Optional<B> asOptional();

    static class Left<L, R> extends Either<L, R> {
        private L value;

        public Left(L v) {
            this.value = v;
        }

        @Override
        public boolean isRight() {
            return false;
        }

        @Override
        public L getLeft() {
            return value;
        }

        @Override
        public R getRight() {
            throw new IllegalStateException("This is Left, no right value");
        }

        @Override
        public Optional<R> asOptional() {
            return Optional.empty();
        }

        @Override
        public String toString() {
            return "Left{" + value + '}';
        }
    }

    static class Right<L, R> extends Either<L, R> {
        private R value;

        public Right(R v) {
            this.value = v;
        }

        @Override
        public boolean isRight() {
            return true;
        }

        @Override
        public L getLeft() {
            throw new IllegalStateException("This is Left, no right value");
        }

        @Override
        public R getRight() {
            return value;
        }

        @Override
        public Optional<R> asOptional() {
            return Optional.of(value);
        }

        @Override
        public String toString() {
            return "Right{" + value + '}';
        }
    }

}


