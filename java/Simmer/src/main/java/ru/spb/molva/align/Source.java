package ru.spb.molva.align;

/**
 * Created by lonlylocly on 4/16/15.
 */
public class Source {

    private int sourceMd5;
    private int count;
    private String text;

    private Word word;

    public int getSourceMd5() {
        return sourceMd5;
    }

    public void setSourceMd5(int sourceMd5) {
        this.sourceMd5 = sourceMd5;
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

    public Word getWord() {
        return word;
    }

    public void setWord(Word word) {
        this.word = word;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Source source = (Source) o;

        if (sourceMd5 != source.sourceMd5) return false;
        return !(word != null ? !word.equals(source.word) : source.word != null);

    }

    @Override
    public int hashCode() {
        int result = sourceMd5;
        result = 31 * result + (word != null ? word.hashCode() : 0);
        return result;
    }
}
