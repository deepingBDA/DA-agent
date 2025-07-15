import type { Config } from 'tailwindcss'
// eslint-disable-next-line import/no-extraneous-dependencies
import plugin from 'tailwindcss/plugin'

export default {
  content: ['./src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    fontSize: {
      15: '8px',
      25: '10px',
      50: '11px',
      75: '12px',
      100: '13px',
      200: '15px',
      300: '17px',
      400: '19px',
      500: '21px',
      600: '24px',
      700: '27px',
      800: '30px',
      900: '34px',
      1000: '38px',
      1100: '43px',
    },
    colors: {
      // https://www.figma.com/file/nCUxrIaN0LAu4Rj9DRxRzn/%F0%9F%8C%B1PI-Atom?node-id=4%3A1298
      transparent: 'transparent',
      current: 'currentColor',
      white: '#ffffff',
      black: '#000000',
      grey: {
        25: '#F9F9FB',
        50: '#E9E9EA',
        100: '#D9DBDD',
        200: '#B1B5BB',
        300: '#8E949D',
        400: '#787F89',
        500: '#565F6C',
        600: '#4E5662',
        700: '#3D434D',
        800: '#2F343B',
        900: '#24282D',
      },
      blue: {
        50: '#EBF0FC',
        100: '#C1D1F6',
        200: '#A3BAF2',
        300: '#799BEC',
        400: '#5F88E8',
        500: '#376AE2',
        600: '#3260CE',
        700: '#274BA0',
        800: '#1E3A7C',
        900: '#172D5F',
      },
      cyan: {
        50: '#F1FAFA',
        100: '#D5EFF0',
        200: '#C0E8E9',
        300: '#A3DDDF',
        400: '#91D6D9',
        500: '#76CCCF',
        600: '#6BBABC',
        700: '#549193',
        800: '#417072',
        900: '#325657',
      },
      purple: {
        50: '#F8EFFC',
        100: '#E8CCF6',
        200: '#DDB4F1',
        300: '#CE91EB',
        400: '#C57CE7',
        500: '#B65BE1',
        600: '#A653CD',
        700: '#8141A0',
        800: '#64327C',
        900: '#4C265F',
      },
      red: {
        50: '#FCEEEE',
        100: '#F5CCCC',
        200: '#F1B3B3',
        300: '#EA9090',
        400: '#E67A7A',
        500: '#E05959',
        600: '#CC5151',
        700: '#9F3F3F',
        800: '#7B3131',
        900: '#5E2525',
      },
      yellow: {
        50: '#FFFBEB',
        100: '#FEF8E0',
        200: '#FDF1BF',
        300: '#FBE68D',
        400: '#FADE66',
        500: '#FAD232',
        600: '#E1BD2D',
        700: '#C8A828',
        800: '#967E1E',
        900: '#705E16',
      },
      green: {
        50: '#F4FFF8',
        100: '#E6FFEF',
        200: '#DFF1E7',
        300: '#CAE8D6',
        400: '#74D69B',
        500: '#5EB982',
        600: '#4A9C6A',
        700: '#387F54',
        800: '#28623F',
        900: '#1A442B',
      },
    },
    extend: {
      fontFamily: {
        pretendardkr: ['pretendard-kr'],
        pretendardJp: ['pretendard-jp'],
      },
      boxShadow: {
        sky: '0px 0px 0px 2px rgba(122, 197, 255, 0.3)',
      },
      width: {
        fit: 'fit-content',
      },
      borderWidth: {
        1: '1px',
      },
      padding: {
        l: '24px',
      },
    },
  },
  corePlugins: {
    preflight: false,
  },
  plugins: [
    plugin(function ({ addUtilities }) {
      addUtilities({
        '.heading-2xs': { 'font-size': '13px', 'line-height': '1.5' },
        '.heading-xs': { 'font-size': '15px', 'line-height': '1.5' },
        '.heading-s': { 'font-size': '17px', 'line-height': '1.5' },
        '.heading-m': { 'font-size': '21px', 'line-height': '1.5' },
        '.heading-l': { 'font-size': '24px', 'line-height': '1.5' },
        '.heading-xl': { 'font-size': '27px', 'line-height': '1.5' },
        '.heading-2xl': { 'font-size': '34px', 'line-height': '1.5' },
        '.heading-3xl': { 'font-size': '38px', 'line-height': '1.5' },
      })
    }),
    plugin(function ({ addUtilities }) {
      addUtilities({
        '.body-s': { 'font-size': '12px', 'line-height': '1.7' },
        '.body-m': { 'font-size': '13px', 'line-height': '1.7' },
        '.body-l': { 'font-size': '15px', 'line-height': '1.7' },
        '.body-xl': { 'font-size': '17px', 'line-height': '1.7' },
      })
    }),
    plugin(function ({ addUtilities }) {
      addUtilities({
        '.keep-all': { 'word-break': 'keep-all' },
        '.break-word': { 'word-break': 'break-word' },
      })
    }),
  ],
} satisfies Config
