# zync-houdini

To install plugin one has to take the following steps:

1. Clone this repository into your machine.

2. Find your `houdini.env` file. [How to find houdini.env.](http://www.sidefx.com/docs/houdini9.5/basics/config_env)
   Add the following section to this file.

    ```
    # zync config start
    HOUDINI_PATH = "/path/to/zync-houdini/;&"
    # zync config end
    ```

    Be careful about preserving `;&` at the end. If you are using Windows make
    sure you are using forward slashes `/` instead of backslashes. Ex. 
    `Z:/plugins/zync-houdini` Remember to replace path with the proper path to
    zync-houdini directory.

3. Clone `zync-python` repository. Then, take one of these steps:
    * Add `ZYNC_API_DIR` environment variable containing path to zync-python.
    * Create `config_houdini.py` and put path to `zync-python` in `API_DIR` variable.

4. Restart Houdini.
