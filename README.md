# Flashcards

Study decks on any topic, with spaced repetition: each card comes back right before you'd forget it.

- **App:** one static page (`index.html`) on GitHub Pages. Sign in with Google or an email code. Friends can sign up and keep their own decks.
- **Data:** stored in the `flashcards_decks` and `flashcards_cards` tables in the shared **WheelOfWhimsy** Supabase project. Row-level security keeps each person's decks private to them.
- **Starter decks:** system design (caching, hashing, databases, Kafka), geography (continent quiz and a map quiz), niche Minecraft, Tolkien, and fixing a bike flat mid-race. They live in `decks/` and are built by `tools/build_decks.py`. Tap **Add** to copy one into your account.
- **Card types:** basic (reveal the answer), multiple choice, and map (pick the country under the pin).
- **Adding cards:** paste `front | back`, one card per line, or `front | right | wrong; wrong; wrong` for multiple choice. Tab-separated rows copied from a spreadsheet work too. With the Supabase connector on, Claude can also build a deck for you.
- **Studying:** after revealing a card, rate it Again, Hard, Good or Easy. On a computer, press space to reveal and 1–4 to rate.
