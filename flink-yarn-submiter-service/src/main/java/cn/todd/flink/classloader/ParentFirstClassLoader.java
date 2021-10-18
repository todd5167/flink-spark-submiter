package cn.todd.flink.classloader;

import java.net.URL;
import java.util.function.Consumer;

/** Regular URLClassLoader that first loads from the parent and only after that from the URLs. */
public class ParentFirstClassLoader extends AbsSubmitterClassLoader {
    static {
        ClassLoader.registerAsParallelCapable();
    }

    ParentFirstClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent, NOOP_EXCEPTION_HANDLER);
    }

    ParentFirstClassLoader(
            URL[] urls, ClassLoader parent, Consumer<Throwable> classLoadingExceptionHandler) {
        super(urls, parent, classLoadingExceptionHandler);
    }
}
