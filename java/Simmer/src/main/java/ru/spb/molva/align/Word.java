package ru.spb.molva.align;

/**
 * Created by lonlylocly on 4/16/15.
 */
public class Word {
    private int wordMd5;
    private int count;
    private String text;

    public int getWordMd5() {
        return wordMd5;
    }

    public void setWordMd5(int wordMd5) {
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
        return wordMd5;
    }
}
