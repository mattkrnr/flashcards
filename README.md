# Flashcards

Study decks on any topic, with spaced repetition: each card comes back right before you'd forget it.

- **App:** one static page (`index.html`) on GitHub Pages. Sign in with Google or an email code. Friends can sign up and keep their own decks.
- **Data:** stored in the `flashcards_decks` and `flashcards_cards` tables in the shared **WheelOfWhimsy** Supabase project. Row-level security keeps each person's decks private to them.
- **Adding cards:** paste `front | back`, one card per line. Tab-separated rows copied from a spreadsheet work too. With the Supabase connector on, Claude can also build a deck for you.
- **Studying:** after revealing a card, rate it Again, Hard, Good or Easy. On a computer, press space to reveal and 1–4 to rate.
