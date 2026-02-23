import js from '@eslint/js'
import globals from 'globals'
import reactPlugin from 'eslint-plugin-react'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'
import eslintPluginPrettierRecommended from 'eslint-plugin-prettier/recommended'
import eslintPluginUnicorn from 'eslint-plugin-unicorn'
import eslintPluginUnusedImports from 'eslint-plugin-unused-imports'
import eslintPluginImportX from 'eslint-plugin-import-x'
// import eslintPluginSonarjs from 'eslint-plugin-sonarjs'
import eslintPluginI18n from 'eslint-plugin-i18n'
import jsxA11y from 'eslint-plugin-jsx-a11y'

export default tseslint.config(
  {
    ignores: [
      'node_modules',
      'dist',
      'public/charting_library',
      'src/charting_library',
      'apps/*/dist',
      'packages/*/dist',
      '**/node_modules/**',
      '**/.git/**',
      '**/dist/**',
      '**/build/**',
      '**/.cache/**',
    ],
  },
  {
    extends: [
      js.configs.recommended,
      ...tseslint.configs.recommended,
      eslintPluginPrettierRecommended,
      reactPlugin.configs.flat.recommended,
      reactPlugin.configs.flat['jsx-runtime'],
      jsxA11y.flatConfigs.recommended,
    ],
    files: ['**/*.{ts,tsx}'],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    settings: {
      react: {
        version: 'detect',
      },
    },
    plugins: {
      react: reactPlugin,
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
      unicorn: eslintPluginUnicorn,
      'unused-imports': eslintPluginUnusedImports,
      'import-x': eslintPluginImportX,
      // sonarjs: eslintPluginSonarjs,
      i18n: eslintPluginI18n,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          vars: 'all',
          varsIgnorePattern: '^_',
          args: 'after-used',
          argsIgnorePattern: '^_',
        },
      ],
      '@typescript-eslint/no-floating-promises': 'error',

      // Unused Imports
      'unused-imports/no-unused-imports': 'error',
      'unused-imports/no-unused-vars': [
        'warn',
        {
          vars: 'all',
          varsIgnorePattern: '^_',
          args: 'after-used',
          argsIgnorePattern: '^_',
        },
      ],

      // Import X
      'import-x/no-cycle': 'error',
      'import-x/order': [
        'error',
        {
          groups: ['builtin', 'external', 'internal', 'parent', 'sibling', 'index'],
          'newlines-between': 'always',
          alphabetize: { order: 'asc', caseInsensitive: true },
        },
      ],

      // SonarJS
      // 'sonarjs/cognitive-complexity': 'off',
      // 'sonarjs/no-identical-expressions': 'warn',

      // I18N
      'i18n/no-russian-character': ['error', { includeComment: true }],

      'unicorn/filename-case': [
        'error',
        {
          case: 'kebabCase',
          ignore: [
            // Ignored files (if any specific patterns needed, e.g. README.md is usually ignored by default or global ignores)
          ],
        },
      ],
      'max-classes-per-file': ['error', 1],
      'no-duplicate-imports': 'error',
      'no-var': 'error',
      'prefer-const': 'error',
      '@typescript-eslint/no-use-before-define': ['error', { functions: false }],
      'lines-between-class-members': 'error',
      'spaced-comment': ['error', 'always'],
      'object-shorthand': 'error',
      'prefer-template': 'error',
      'no-return-await': 'error',
      'no-sequences': 'error',
      'no-implicit-coercion': ['error', { allow: ['!!'] }],
      'eqeqeq': ['error', 'always'],
      'react/destructuring-assignment': ['error', 'always'],
      'react/prop-types': 'off',
      'react/jsx-no-leaked-render': ['error', { validStrategies: ['coerce', 'ternary'] }],
      curly: ['error', 'all'],
    },
  },
)
