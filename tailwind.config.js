/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{html,js,svelte,ts}'],
  theme: {
    extend: {
      colors: {
        warm: {
          bg:      '#FDFAF6',
          panel:   '#F5F0E8',
          canvas:  '#FAF7F2',
          border:  '#DDD5C8',
          muted:   '#A0907A',
          text:    '#2C2416',
          sub:     '#6B5A45',
        },
        accent: {
          DEFAULT: '#C07A3A',
          hover:   '#A8672E',
          light:   '#FDE9C8',
        }
      }
    },
  },
  plugins: [],
}
