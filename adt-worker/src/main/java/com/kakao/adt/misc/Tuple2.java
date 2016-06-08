package com.kakao.adt.misc;

public class Tuple2<A, B> {

    private final A a;
    private final B b;
    
    public Tuple2(A a, B b){
        this.a = a;
        this.b = b;
    }
    
    public A getA(){
        return a;
    }
    
    public B getB(){
        return b;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((a == null) ? 0 : a.hashCode());
        result = prime * result + ((b == null) ? 0 : b.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Tuple2)) {
            return false;
        }
        Tuple2 other = (Tuple2) obj;
        if (a == null) {
            if (other.a != null) {
                return false;
            }
        } else if (!a.equals(other.a)) {
            return false;
        }
        if (b == null) {
            if (other.b != null) {
                return false;
            }
        } else if (!b.equals(other.b)) {
            return false;
        }
        return true;
    }
    
    
    
}
