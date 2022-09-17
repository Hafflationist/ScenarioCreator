package de.mrobohm.utils;

import java.util.Optional;

public abstract class PersistentStack<T> {

    public static <TCreate> PersistentStack<TCreate> getEmpty() {
        return new EmptyNode<>();
    }

    public abstract Optional<PersistentStack<T>> pop();

    public abstract Optional<T> peek();

    public abstract boolean isEmpty();

    public PersistentStack<T> push(T element) {
        return new LinkNode(this, element);
    }

    private static class EmptyNode<TEmpty> extends PersistentStack<TEmpty> {
        @Override
        public Optional<PersistentStack<TEmpty>> pop() {
            return Optional.empty();
        }

        @Override
        public Optional<TEmpty> peek() {
            return Optional.empty();
        }

        @Override
        public boolean isEmpty() {
            return true;
        }
    }

    private class LinkNode extends PersistentStack<T> {
        final PersistentStack<T> previous;
        final T element;

        public LinkNode(PersistentStack<T> previous, T element) {
            this.previous = previous;
            this.element = element;
        }

        @Override
        public Optional<PersistentStack<T>> pop() {
            return Optional.of(previous);
        }

        @Override
        public Optional<T> peek() {
            return Optional.of(element);
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }
}