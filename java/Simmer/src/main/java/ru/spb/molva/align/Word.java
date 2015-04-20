package ru.spb.molva.align;

/**
 * Created by lonlylocly on 4/16/15.
 */
public class Word {
    private long wordMd5 = 0;
    private int count = 0;
    private String text = "";

    public Word() {
    }

    public Word(long wordMd5, int count, String text) {
        this.wordMd5 = wordMd5;
        this.count = count;
        this.text = text;
    }

    public Word(long wordMd5) {
        this.wordMd5 = wordMd5;
    }

    public long getWordMd5() {
        return wordMd5;
    }

    public void setWordMd5(long wordMd5) {
        this.wordMd5 = wordMd5;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Word word = (Word) o;

        return wordMd5 == word.wordMd5;

    }

    @Override
    public int hashCode() {
        return (int) (wordMd5 ^ (wordMd5 >>> 32));
    }

    @Override
    public String toString() {
        return "Word{" +
                "wordMd5=" + wordMd5 +
                ", count=" + count +
                ", text='" + text + '\'' +
                '}';
    }
}
