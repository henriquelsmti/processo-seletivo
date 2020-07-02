import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

	
	public class Registro {
		String regiao;
		String siglaEstado;
		String siglaMunicipio;
		String revendaInstalacao;
		String codigoProduto;
		String nomeProduto;
		String unidadeDeMedida;
		String bandeira;
		Double valorDeCompra;
		Double valorDeVenda;
		Date dataDaColeta;
	}

	public static void importFile(File file) {
		try {
			final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
			List<Registro> registros;
			if (file.getName().toLowerCase().endsWith(".json")) {
				ObjectMapper mapper = new ObjectMapper();
				mapper.setDateFormat(dateFormat);

				mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
						.withFieldVisibility(JsonAutoDetect.Visibility.ANY)
						.withGetterVisibility(JsonAutoDetect.Visibility.NONE)
						.withSetterVisibility(JsonAutoDetect.Visibility.NONE)
						.withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
				
				CollectionType collectionType = mapper.getTypeFactory().constructCollectionType(List.class, Registro.class);
				
				registros = mapper.readValue(file.toURI().toURL(), collectionType);
			} else if (file.getName().toLowerCase().endsWith(".csv")) { 
				List<String> lines = Files.readAllLines(Paths.get(file.toURI()));
				registros = new ArrayList<>();
				for (String line : lines){
					if (!line.equals("regiao|siglaEstado|siglaMunicipio|revendaInstalacao|codigoProduto|nomeProduto|dataDaColeta|valorDeCompra|valorDeVenda|unidadeDeMedida|bandeira")) {
						String[] colunas = line.split("\\|");
						
						Registro registro = new Registro();
						registro.regiao = colunas[0];
						registro.siglaEstado = colunas[1];
						registro.siglaMunicipio = colunas[2];
						registro.revendaInstalacao = colunas[3];
						registro.codigoProduto = colunas[4];
						registro.nomeProduto = colunas[5];
						try {
							registro.dataDaColeta = dateFormat.parse(colunas[6]);
						} catch (ParseException e) {
							throw new RuntimeException(e);
						}
						registro.valorDeCompra = !colunas[7].equals("") ? Double.parseDouble(colunas[7]) : null;
						registro.valorDeVenda = Double.parseDouble(colunas[8]);
						registro.unidadeDeMedida = colunas[9];
						registro.bandeira = colunas[10];
						registros.add(registro);
					}
				}
			} else {
				throw new RuntimeException("Arquivo invalido");
			}
			
			String url = "jdbc:postgresql://localhost/banco_precos";
			String user = "postgres";
			String password = "fibo123";
			
			Connection connection = DriverManager.getConnection(url, user, password);
			
			for (Registro registro : registros) {
				
				PreparedStatement st = connection.prepareStatement(
						"INSERT INTO precos(regiao, siglaestado, siglamunicipio, revendainstalacao, codigoproduto, nomeproduto, unidadedemedida, bandeira, valordecompra, valordevenda, datadacoleta)\n" +
								"VALUES (?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?)"
				);
				
				st.setString(1, registro.regiao);
				st.setString(2, registro.siglaEstado);
				st.setString(3, registro.siglaMunicipio);
				st.setString(4, registro.revendaInstalacao);
				st.setString(5, registro.codigoProduto);
				st.setString(6, registro.nomeProduto);
				st.setString(7, registro.unidadeDeMedida);
				st.setString(8, registro.bandeira);
				st.setObject(9, registro.valorDeCompra);
				st.setDouble(10, registro.valorDeVenda);
				st.setDate(11, new java.sql.Date(registro.dataDaColeta.getTime()));
				
				st.executeUpdate();
				st.close();
			}

			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static List<Registro> listAll() {
		try {
			List<Registro> registros = new ArrayList<>();
			
			String url = "jdbc:postgresql://localhost/banco_precos";
			String user = "postgres";
			String password = "fibo123";
			
			Connection connection = DriverManager.getConnection(url, user, password);
			
			PreparedStatement st = connection.prepareStatement(
					"SELECT * FROM precos"
			);
			
			ResultSet resultSet = st.executeQuery();
			
			while (resultSet.next()) {
				
				Registro registro = new Registro();
				registro.regiao = resultSet.getString("regiao");
				registro.siglaEstado = resultSet.getString("siglaestado");
				registro.siglaMunicipio = resultSet.getString("siglamunicipio");
				registro.revendaInstalacao = resultSet.getString("revendainstalacao");
				registro.codigoProduto = resultSet.getString("codigoproduto");
				registro.nomeProduto = resultSet.getString("nomeproduto");
				registro.dataDaColeta = resultSet.getDate("dataDacoleta");
				registro.valorDeCompra = resultSet.getDouble("valordecompra");
				registro.valorDeVenda = resultSet.getDouble("valordevenda");
				registro.unidadeDeMedida = resultSet.getString("unidadedemedida");
				registro.bandeira = resultSet.getString("bandeira");
				
				registros.add(registro);
			}
			st.close();
			resultSet.close();
			connection.close();
			return registros;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
