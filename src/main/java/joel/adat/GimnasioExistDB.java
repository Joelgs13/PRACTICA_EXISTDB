package joel.adat;

import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.ResourceIterator;
import org.xmldb.api.base.ResourceSet;
import org.xmldb.api.modules.XMLResource;
import org.xmldb.api.modules.CollectionManagementService;
import org.xmldb.api.base.Database;
import org.xmldb.api.modules.XQueryService;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.lang.System.out;

public class GimnasioExistDB {
    private static final String URI = "xmldb:exist://localhost:8080/exist/xmlrpc/db";
    private static final String USER = "admin";
    private static final String PASSWORD = "";

    public static void main(String[] args) throws Exception {
        // 1. Registrar la base de datos
        Class<?> cl = Class.forName("org.exist.xmldb.DatabaseImpl");
        Database database = (Database) cl.getDeclaredConstructor().newInstance();
        DatabaseManager.registerDatabase(database);
        System.out.println("Conexión exitosa a la base de datos: " + URI);

        // 2. Conectar a la colección raíz
        Collection root = DatabaseManager.getCollection(URI, USER, PASSWORD);
        if (root == null) {
            System.err.println("No se pudo conectar con la colección raíz. Verifica el URI, usuario y contraseña.");
            return;  // Detener la ejecución si no se puede acceder al servidor
        }

        System.out.println("Conexión exitosa a la base de datos. Verificando colección GIMNASIO...");

        // 3. Verificar si la colección GIMNASIO existe, de lo contrario, crearla
        Collection gimnasioCollection = root.getChildCollection("GIMNASIO/");
        System.out.println(root.getChildCollection("GIMNASIO/"));
        if (gimnasioCollection != null) {
            out.println("La colección 'GIMNASIO' ya existe.");
        } else {
            CollectionManagementService mgtService = (CollectionManagementService) root.getService("CollectionManagementService", "1.0");
            gimnasioCollection = mgtService.createCollection("GIMNASIO");
            out.println("Colección 'GIMNASIO' creada.");
        }

        // 4. Subir documentos XML desde src/main/resources/xml
        subirDocumento(gimnasioCollection, "src/main/resources/xml/socios_gim.xml");
        subirDocumento(gimnasioCollection, "src/main/resources/xml/actividades_gim.xml");
        subirDocumento(gimnasioCollection, "src/main/resources/xml/uso_gimnasio.xml");

        // 5. Calcular y generar documento intermedio
        String xqueryIntermedio = generarXQueryIntermedio();
        ejecutarYGuardarXQuery(gimnasioCollection, xqueryIntermedio, "cuotas_adicionales.xml");

        // 6. Calcular cuota final
        String xqueryFinal = generarXQueryFinal();
        ejecutarYGuardarXQuery(gimnasioCollection, xqueryFinal, "cuotas_finales.xml");

        out.println("Ejercicio completado. Documentos generados y guardados.");
    }

    private static void subirDocumento(Collection col, String filePath) throws Exception {
        // Accede al archivo desde el classpath
        File archivo= new File(filePath);
        if(!archivo.canRead()) System.out.println("ERROR AL LEER EL FICHERO");
        else
        { Resource nuevoRecurso = col.createResource(archivo.getName(), "XMLResource");
            nuevoRecurso.setContent(archivo); //comprueba si es un archivo
            col.storeResource(nuevoRecurso);
            out.println("Documento subido: " + filePath);
        }
    }

    private static String generarXQueryIntermedio() {
        return """
    let $socios := doc('socios_gim.xml')/SOCIOS_GIM/fila_socios
    let $actividades := doc('actividades_gim.xml')/ACTIVIDADES_GIM/fila_actividades
    let $uso := doc('uso_gimnasio.xml')/USO_GIMNASIO/fila_uso
    for $u in $uso
    let $socio := $socios[COD=$u/CODSOCIO]
    let $actividad := $actividades[@cod=$u/CODACTIV]
    let $horas := xs:integer($u/HORAFINAL) - xs:integer($u/HORAINICIO)
    let $cuota_adicional := 
        if ($actividad/@tipo='1') then 0
        else if ($actividad/@tipo='2') then $horas * 2
        else if ($actividad/@tipo='3') then $horas * 4
        else 0
    return 
        <datos>
            <COD>{$u/CODSOCIO}</COD>
            <NOMBRESOCIO>{$socio/NOMBRE}</NOMBRESOCIO>
            <CODACTIV>{$u/CODACTIV}</CODACTIV>
            <NOMBREACTIVIDAD>{$actividad/NOMBRE}</NOMBREACTIVIDAD>
            <horas>{$horas}</horas>
            <tipoact>{$actividad/@tipo}</tipoact>
            <cuota_adicional>{$cuota_adicional}</cuota_adicional>
        </datos>
    """;
    }


    private static String generarXQueryFinal() {
        return """
        let $socios := doc('socios_gim.xml')/SOCIOS_GIM/fila_socios
        let $cuotas := doc('cuotas_adicionales.xml')/datos
        for $socio in $socios
        let $suma_cuota_adic := sum($cuotas[COD=$socio/COD]/cuota_adicional)
        let $cuota_total := $suma_cuota_adic + xs:decimal($socio/CUOTA_FIJA)
        return 
            <datos>
                <COD>{$socio/COD}</COD>
                <NOMBRESOCIO>{$socio/NOMBRE}</NOMBRESOCIO>
                <CUOTA_FIJA>{$socio/CUOTA_FIJA}</CUOTA_FIJA>
                <suma_cuota_adic>{$suma_cuota_adic}</suma_cuota_adic>
                <cuota_total>{$cuota_total}</cuota_total>
            </datos>
        """;
    }

    private static void ejecutarYGuardarXQuery(Collection col, String xquery, String fileName) throws Exception {
        XQueryService xQueryService = (XQueryService) col.getService("XQueryService", "1.0");
        ResourceSet result = xQueryService.query(xquery);
        XMLResource resource = (XMLResource) col.createResource(fileName, "XMLResource");
        StringBuilder content = new StringBuilder("<result>");
        ResourceIterator iter = result.getIterator();
        while (iter.hasMoreResources()) {
            Resource r = iter.nextResource();
            content.append(r.getContent());
        }
        content.append("</result>");
        resource.setContent(content.toString());
        col.storeResource(resource);
        out.println("Documento guardado: " + fileName);
    }
}
