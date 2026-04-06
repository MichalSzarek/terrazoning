/** @type {import('tailwindcss').Config} */
export default {
  // Dark mode always-on via 'class' strategy — we set 'dark' on <html> in main.tsx
  darkMode: 'class',
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx}',
  ],
  theme: {
    extend: {
      colors: {
        // TerraZoning brand tokens
        surface: {
          DEFAULT: '#0d1117',   // deepest background
          raised: '#161b22',    // panels, cards
          elevated: '#21262d',  // hover states, inputs
        },
        border: {
          DEFAULT: '#30363d',   // default border
          muted: '#21262d',     // subtle dividers
        },
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'ui-monospace', 'monospace'],
      },
    },
  },
  plugins: [],
};
