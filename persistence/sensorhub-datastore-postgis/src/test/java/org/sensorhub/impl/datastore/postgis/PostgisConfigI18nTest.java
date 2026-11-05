package org.sensorhub.impl.datastore.postgis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.database.OscarTieredObsSystemDatabaseConfig;
import org.sensorhub.impl.datastore.postgis.database.PostgisDatabaseConfig;
import org.sensorhub.impl.datastore.postgis.database.PostgisObsSystemDatabaseConfig;


public class PostgisConfigI18nTest
{
    private static final List<String> BUNDLE_PATHS = Arrays.asList(
        "/org/sensorhub/impl/datastore/postgis/i18n/ConfigMessages",
        "/org/sensorhub/impl/datastore/postgis/database/i18n/ConfigMessages",
        "/org/sensorhub/impl/datastore/postgis/archive/i18n/ConfigMessages");

    private static final List<Class<?>> CONFIG_CLASSES = Arrays.asList(
        PostgisDatabaseConfig.class,
        PostgisObsSystemDatabaseConfig.class,
        OscarTieredObsSystemDatabaseConfig.class,
        TieredArchiveConfig.class);


    @Test
    public void postgisBundlesContainEveryEnglishKeyInAllLocales() throws Exception
    {
        for (String basePath : BUNDLE_PATHS)
        {
            Properties en = loadProperties(basePath + ".properties");
            Properties es = loadProperties(basePath + "_es.properties");
            Properties fr = loadProperties(basePath + "_fr.properties");
            Properties el = loadProperties(basePath + "_el.properties");

            assertEquals(basePath + " Spanish key mismatch", en.keySet(), es.keySet());
            assertEquals(basePath + " French key mismatch", en.keySet(), fr.keySet());
            assertEquals(basePath + " Greek key mismatch", en.keySet(), el.keySet());
        }
    }


    @Test
    public void everyDeclaredPostgisConfigFieldHasLabelAndDescription() throws Exception
    {
        for (Class<?> configClass : CONFIG_CLASSES)
        {
            String basePath = "/" + configClass.getPackageName().replace('.', '/') + "/i18n/ConfigMessages.properties";
            Properties en = loadProperties(basePath);
            for (Field field : configClass.getDeclaredFields())
            {
                if (!Modifier.isPublic(field.getModifiers()) || Modifier.isStatic(field.getModifiers()))
                    continue;
                String prefix = configClass.getSimpleName() + "." + field.getName();
                assertNotNull("Missing label for " + prefix, en.getProperty(prefix + ".label"));
                assertNotNull("Missing description for " + prefix, en.getProperty(prefix + ".description"));
            }
        }
    }


    private Properties loadProperties(String path) throws Exception
    {
        try (InputStream in = getClass().getResourceAsStream(path))
        {
            assertNotNull("Missing resource " + path, in);
            Properties props = new Properties();
            try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8))
            {
                props.load(reader);
            }
            return props;
        }
    }
}
