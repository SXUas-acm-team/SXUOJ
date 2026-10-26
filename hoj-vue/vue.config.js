const CompressionWebpackPlugin = require('compression-webpack-plugin');
const webpack = require('webpack');
const frontendVersion = require('crypto').randomBytes(16).toString('hex');
module.exports = {
  publicPath: '/',
  assetsDir: 'assets',
  productionSourceMap: false,
  devServer: {
    open: true,
    host: '0.0.0.0',
    port: 8066,
    proxy: { '/api': { target: 'http://127.0.0.1:6688', changeOrigin: true } },
  },
  configureWebpack(config) {
    config.performance = { maxEntrypointSize: 10000000, maxAssetSize: 30000000 };
    if (process.env.NODE_ENV === 'production') {
      config.plugins.push(new webpack.DefinePlugin({
        'process.env.VUE_APP_BUILD_VERSION': JSON.stringify(frontendVersion),
      }));
      config.plugins.push({
        apply(compiler) {
          compiler.hooks.emit.tap('SXUOJVersion', compilation => {
            const content = JSON.stringify({ version: frontendVersion });
            compilation.assets['version.json'] = { source: () => content, size: () => Buffer.byteLength(content) };
          });
        },
      });
      config.plugins.push(new CompressionWebpackPlugin({
        filename: '[path].gz[query]',
        algorithm: 'gzip',
        test: /\.(js|css|html|svg|json)$/,
        threshold: 10240,
        deleteOriginalAssets: false,
        minRatio: 0.8,
      }));
    }
  },
};
